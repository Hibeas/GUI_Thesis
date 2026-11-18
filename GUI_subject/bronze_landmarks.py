"""
Module for handling video capture, MediaPipe landmark extraction,
Kafka streaming (batched & compressed), and MinIO uploading.
"""
import cv2
import threading
import json
import time
import os
import mediapipe as mp
import boto3
from confluent_kafka import Producer
from mediapipe.tasks import python
from mediapipe.tasks.python import vision

import config
from datetime import datetime
from zoneinfo import ZoneInfo

WARSAW_TZ = ZoneInfo("Europe/Warsaw")

def get_warsaw_timestamp_ms() -> int:
    now_pl = datetime.now(WARSAW_TZ)
    offset_sec = now_pl.utcoffset().total_seconds()
    return int((time.time() + offset_sec) * 1000)

latest_frame = None
frame_lock = threading.Lock()
stop_event = threading.Event()

upload_status = {
    "is_uploading": False,
    "completed": 0,
    "total": 0
}

s3_client = boto3.client(
    's3',
    endpoint_url=config.MINIO_URL,
    aws_access_key_id=config.MINIO_ACCESS_KEY,
    aws_secret_access_key=config.MINIO_SECRET_KEY
)

# Zoptymalizowany producent Kafki: grupowanie pakietów co 50ms + szybka kompresja LZ4
producer = Producer({
    'bootstrap.servers': config.KAFKA_SERVER,
    'broker.address.family': 'v4',
    'compression.type': 'lz4',
    'linger.ms': 50,
    'batch.size': 65536,
    'acks': 1
})

os.makedirs(config.TEMP_DIR, exist_ok=True)

def delivery_report(err, msg):
    if err is not None:
        print(f"[LANDMARKS KAFKA ERROR] {err}")

# W bronze_landmarks.py zamiast dotychczasowego upload_video_segment:

def process_pending_uploads(is_recording_fn):
    """
    Skanuje TEMP_DIR i wysyła zaległe segmenty wideo do MinIO TYLKO wtedy,
    gdy aktualnie nie trwa nagrywanie (is_recording_fn() == False).
    Aktualizuje upload_status na potrzeby GUI.
    """
    global upload_status

    if is_recording_fn() or not os.path.exists(config.TEMP_DIR):
        upload_status = {"is_uploading": False, "completed": 0, "total": 0}
        return

    # 1. Zbieramy listę wszystkich plików oczekujących na wysłanie
    pending_files = []
    for participant_dir in sorted(os.listdir(config.TEMP_DIR)):
        full_dir_path = os.path.join(config.TEMP_DIR, participant_dir)
        if os.path.isdir(full_dir_path):
            for filename in sorted(os.listdir(full_dir_path)):
                file_path = os.path.join(full_dir_path, filename)
                if os.path.isfile(file_path):
                    pending_files.append((participant_dir, filename, file_path, full_dir_path))

    total_files = len(pending_files)
    if total_files == 0:
        upload_status = {"is_uploading": False, "completed": 0, "total": 0}
        return

    upload_status = {"is_uploading": True, "completed": 0, "total": total_files}

    for idx, (participant_dir, filename, file_path, full_dir_path) in enumerate(pending_files, start=1):
        if is_recording_fn():
            print("[MINIO] Wykryto start nowego eksperymentu — wstrzymuję wysyłanie wideo w tle.")
            break

        try:
            s3_key = f"video/{participant_dir}/{filename}"
            s3_client.upload_file(file_path, config.BUCKET_NAME, s3_key)
            os.remove(file_path)
            upload_status["completed"] = idx
            print(f"[MINIO] Wysłano ({idx}/{total_files}) i usunięto lokalnie: {s3_key}")
        except Exception as e:
            print(f"[MINIO ERROR] Błąd wysyłania {filename} (plik zachowany na dysku): {e}")

        # Usuń pusty folder uczestnika po wysłaniu jego plików
        try:
            if os.path.exists(full_dir_path) and not os.listdir(full_dir_path):
                os.rmdir(full_dir_path)
        except OSError:
            pass

    upload_status = {"is_uploading": False, "completed": 0, "total": 0}

def run_landmarks(session_config: dict):
    global latest_frame

    participant_id = session_config.get("participation_id") or session_config.get("subject_id", "unknown_subject")

    base_options = python.BaseOptions(model_asset_path=config.MODEL_PATH)
    options = vision.FaceLandmarkerOptions(
        base_options=base_options,
        output_face_blendshapes=True,
        output_facial_transformation_matrixes=True,
        num_faces=1
    )
    detector = vision.FaceLandmarker.create_from_options(options)

    cap = cv2.VideoCapture(0)
    fourcc = cv2.VideoWriter_fourcc(*'XVID')
    jpeg_params = [int(cv2.IMWRITE_JPEG_QUALITY), 75]

    participant_temp_dir = os.path.join(config.TEMP_DIR, str(participant_id))
    os.makedirs(participant_temp_dir, exist_ok=True)

    print(f"[CAMERA] Uruchomiono kamerę dla: {participant_id}")
    stop_event.clear()

    try:
        while cap.isOpened() and not stop_event.is_set():
            ret, frame = cap.read()
            if not ret:
                break

            timestamp = int(get_warsaw_timestamp_ms() / 1000)
            filename = f"vid_{timestamp}.avi"
            local_path = os.path.join(participant_temp_dir, filename)

            out = cv2.VideoWriter(local_path, fourcc, 20.0, (640, 480))
            segment_end_time = time.time() + config.SEGMENT_DURATION

            while time.time() < segment_end_time and not stop_event.is_set():
                ret, frame = cap.read()
                if not ret:
                    break

                out.write(frame)

                rgb_frame = cv2.cvtColor(frame, cv2.COLOR_BGR2RGB)
                mp_image = mp.Image(image_format=mp.ImageFormat.SRGB, data=rgb_frame)
                detection_result = detector.detect(mp_image)

                blendshapes_data = {}
                if detection_result.face_blendshapes:
                    blendshapes_data = {
                        b.category_name: round(float(b.score), 3)
                        for b in detection_result.face_blendshapes[0]
                    }

                    coords = [
                        {"x": round(l.x, 5), "y": round(l.y, 5), "z": round(l.z, 5)}
                        for l in detection_result.face_landmarks[0]
                    ]

                    data = {
                        "participation_id": participant_id,
                        "ts": get_warsaw_timestamp_ms(),
                        "landmarks": coords,
                        "blendshapes": blendshapes_data
                    }

                    producer.produce(
                        config.KAFKA_TOPIC_LANDMARKS,
                        json.dumps(data, separators=(',', ':')).encode('utf-8'),
                        callback=delivery_report
                    )
                    producer.poll(0)

                if blendshapes_data:
                    smile = blendshapes_data.get('mouthSmileLeft', 0)
                    cv2.putText(frame, f"SMILE: {int(smile * 100)}%", (20, 50), 2, 1, (0, 255, 0), 2)

                ret, buffer = cv2.imencode('.jpg', frame, jpeg_params)
                if ret:
                    with frame_lock:
                        latest_frame = buffer.tobytes()

                time.sleep(0.005)

            out.release()

    except Exception as e:
        print(f"[CAMERA PIPELINE ERROR] {e}")
    finally:
        cap.release()
        producer.flush()
        print("[CAMERA] Kamera oraz strumień landmarków zostały bezpiecznie zatrzymane.")
