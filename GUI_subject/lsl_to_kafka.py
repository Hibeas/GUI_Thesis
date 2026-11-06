"""
Module for streaming LSL biosignals (GSR, PPG) to Kafka.
Integrated into Subject Machine pipeline.
"""
import json
import time
import threading
from confluent_kafka import Producer
from pylsl import StreamInlet, resolve_streams

import config

stop_event = threading.Event()

# Mapowanie docelowych kanałów
TARGET_CHANNELS = {
    "gsr": "GSR_Skin_Conductance",
    "ppg": "PPG_A1"
}

def delivery_report(err, msg):
    if err is not None:
        print(f"[LSL KAFKA ERROR] {err}")

def run_lsl(session_config: dict):
    """
    Pobiera dane z LSL (Consensys/Shimmer) i wysyła je do Kafki.
    Runs until stop_event is set.
    """
    participant_id = session_config.get("subject_id", "unknown_subject")
    
    stop_event.clear()

    producer = Producer({
        'bootstrap.servers': config.KAFKA_SERVER,
        'broker.address.family': 'v4'
    })

    print("[LSL] Skanowanie w poszukiwaniu strumienia LSL Consensys/Shimmer...")
    streams = resolve_streams(wait_time=3.0)

    if not streams:
        print("[LSL WARNING] Nie wykryto strumieni LSL! Pomijanie nagrywania biosygnałów.")
        return

    shimmer_stream = streams[0]
    inlet = StreamInlet(shimmer_stream)
    info = inlet.info()

    print(f"[LSL] Połączono ze strumieniem: '{info.name()}' ({info.channel_count()} kanałów)")

    # DYNAMICZNE MAPOWANIE KANAŁÓW
    ch_elem = info.desc().child("channels").child("channel")
    available_channels = {}

    for idx in range(info.channel_count()):
        label = ch_elem.child_value("label") or ch_elem.child_value("name") or f"Ch_{idx}"
        available_channels[label] = idx
        ch_elem = ch_elem.next_sibling()

    channel_indices = {}
    for json_key, lsl_label in TARGET_CHANNELS.items():
        if lsl_label in available_channels:
            channel_indices[json_key] = (available_channels[lsl_label], lsl_label)

    if not channel_indices:
        print("[LSL ERROR] Brak wymaganych kanałów w strumieniu LSL!")
        return

    print(f"[LSL] Rozpoczynanie wysyłania biosygnałów dla uczestnika: {participant_id}...")

    count = 0
    try:
        while not stop_event.is_set():
            sample, ts_lsl = inlet.pull_sample(timeout=0.5)

            if sample is None:
                continue

            count += 1
            ts_ms = int(ts_lsl * 1000)

            payload = {
                "participation_id": participant_id,
                "ts": ts_ms,
                "hr": 75.0
            }

            for json_key, (idx, _) in channel_indices.items():
                payload[json_key] = round(float(sample[idx]), 2)

            producer.produce(
                'biosignal-data',
                json.dumps(payload).encode('utf-8'),
                callback=delivery_report
            )
            producer.poll(0)

    except Exception as e:
        print(f"[LSL PIPELINE ERROR] {e}")
    finally:
        producer.flush()
        print("[LSL] Strumieniowanie biosygnałów zostało zakończone.")