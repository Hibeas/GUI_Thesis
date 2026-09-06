"""
Module for handling video capture, MediaPipe landmark extraction,
Kafka streaming, and MinIO uploading.
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

# Shared memory variables for FastAPI streaming
latest_frame = None
frame_lock = threading.Lock()
stop_event = threading.Event()

# Initialize S3 Client
s3_client = boto3.client('s3', endpoint_url=config.MINIO_URL, 
                        aws_access_key_id=config.MINIO_ACCESS_KEY, 
                        aws_secret_access_key=config.MINIO_SECRET_KEY)

# Initialize Kafka Producer
producer = Producer({'bootstrap.servers': config.KAFKA_SERVER})

# Ensure temp directory exists
os.makedirs(config.TEMP_DIR, exist_ok=True)

def delivery_report(err, msg):
    """Callback for Kafka producer to report success or failure."""
    if err is not None: 
        print(f"Kafka Error: {err}")

def upload_video_segment(file_path: str, filename: str, participant_id: str):
    """
    Uploads a video segment to MinIO and deletes the local temporary file.
    
    Args:
        file_path (str): The local absolute path to the video file.
        filename (str): The name of the file to be saved in the bucket.
        participant_id (str): The ID of the participant for bucket organization.
    """
    try:
        s3_client.upload_file(file_path, config.BUCKET_NAME, f"video/{participant_id}/{filename}")
        if os.path.exists(file_path): 
            os.remove(file_path)
            print(f"Successfully deleted local temp file: {file_path}")
    except Exception as e: 
        print(f"S3 Error: {e}")

def run_landmarks(session_config: dict):
    """
    Captures webcam feed, extracts MediaPipe landmarks, streams frames to API,
    and publishes data to Kafka.
    
    Args:
        session_config (dict): Dictionary containing 'subject_id' and 'experiment_id'.
    """
    global latest_frame
    
    participant_id = session_config.get("subject_id", "unknown_subject")
    
    # Initialize MediaPipe
    base_options = python.BaseOptions(model_asset_path=config.MODEL_PATH)
    options = vision.FaceLandmarkerOptions(
        base_options=base_options,
        output_face_blendshapes=True,
        output_facial_transformation_matrixes=True,
        num_faces=1)
    detector = vision.FaceLandmarker.create_from_options(options)

    cap = cv2.VideoCapture(0)
    fourcc = cv2.VideoWriter_fourcc(*'XVID')

    print(f"Starting camera for subject: {participant_id}... press 'q' to quit.")
    stop_event.clear()
    
    try:
        while cap.isOpened():
            if stop_event.is_set():
                break

            ret, frame = cap.read()
            if not ret: break
            
            timestamp = int(time.time())
            filename = f"vid_{timestamp}.avi"
            local_path = os.path.join(config.TEMP_DIR, f"temp_{filename}")
            
            out = cv2.VideoWriter(local_path, fourcc, 20.0, (640, 480))
            segment_end_time = time.time() + config.SEGMENT_DURATION
            
            while time.time() < segment_end_time:
                if stop_event.is_set():
                    break
                ret, frame = cap.read()
                if not ret: break
                
                out.write(frame)

                # MediaPipe Processing
                rgb_frame = cv2.cvtColor(frame, cv2.COLOR_BGR2RGB)
                mp_image = mp.Image(image_format=mp.ImageFormat.SRGB, data=rgb_frame)
                detection_result = detector.detect(mp_image)
                
                blendshapes_data = {}
                if detection_result.face_blendshapes:
                    blendshapes_data = {b.category_name: round(float(b.score), 3) 
                            for b in detection_result.face_blendshapes[0]}
                    
                    coords = [{"x": l.x, "y": l.y, "z": l.z} for l in detection_result.face_landmarks[0]]
                    
                    data = {
                        "participation_id": participant_id,
                        "experiment": session_config.get("experiment_id", "default"),
                        "ts": time.time()*1000,
                        "landmarks": coords,
                        "blendshapes": blendshapes_data
                    }
                    
                    producer.produce(config.KAFKA_TOPIC_LANDMARKS, json.dumps(data).encode('utf-8'), callback=delivery_report)
                    producer.poll(0)
                
                # Visuals
                if blendshapes_data:
                    smile = blendshapes_data.get('mouthSmileLeft', 0)
                    cv2.putText(frame, f"SMILE: {int(smile*100)}%", (20, 50), 2, 1, (0,255,0), 2)

                cv2.imshow('Blendshape Ingest', frame)
                
                # Provide frame to FastAPI
                ret, buffer = cv2.imencode('.jpg', frame)
                if ret:
                    with frame_lock:
                        latest_frame = buffer.tobytes()

                if cv2.waitKey(1) & 0xFF == ord('q'):
                    cap.release()
                    out.release()
                    cv2.destroyAllWindows()
                    break

            out.release()
            
            # Start background upload
            threading.Thread(target=upload_video_segment, args=(local_path, filename, participant_id), daemon=True).start()
            
            if not cap.isOpened():
                break

    except Exception as e:
        print(f"Camera Pipeline Error: {e}")
    finally:
        cap.release()
        cv2.destroyAllWindows()
        producer.flush()