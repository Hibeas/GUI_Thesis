"""
Module for streaming LSL biosignals (GSR, PPG) to Kafka.
Integrated into Subject Machine pipeline with LSL clock synchronization.
"""
import json
import time
import threading
from confluent_kafka import Producer
from pylsl import StreamInlet, local_clock, resolve_byprop, resolve_streams

import config

stop_event = threading.Event()
TOPIC_NAME = getattr(config, 'KAFKA_TOPIC_BIOSIGNALS', 'biosignal-data')

TARGET_CHANNELS = {
    "gsr": "GSR_Skin_Conductance",
    "ppg": "PPG_A1",
}

def delivery_report(err, msg):
    if err is not None:
        print(f"[LSL KAFKA ERROR] {err}")

def run_lsl(session_config: dict):
    """
    Pobiera nazwę czujnika z session_config['sensor_name'], skanuje sieć LSL,
    synchronizuje zegar i wysyła próbki do Kafki.
    """
    participation_id = session_config.get("participation_id") or session_config.get("subject_id", "unknown_subject")
    sensor_name = session_config.get("sensor_name")

    if not sensor_name:
        print("[LSL INFO] Nie przypisano czujnika Shimmer dla tej sesji. Pomijanie strumieniowania LSL.")
        return
    
    stop_event.clear()

    producer = Producer({
        'bootstrap.servers': config.KAFKA_SERVER,
        'broker.address.family': 'v4'
    })

    print(f"[LSL] Skanowanie sieci w poszukiwaniu strumienia '{sensor_name}'...")
    streams = resolve_byprop('name', sensor_name, timeout=5.0)

    if not streams:
        print(f"[LSL ERROR] Strumień '{sensor_name}' nie został znaleziony!")
        all_streams = resolve_streams(wait_time=2.0)
        if all_streams:
            available_names = [s.name() for s in all_streams]
            print(f"[LSL INFO] Dostępne strumienie w sieci: {available_names}")
        else:
            print("[LSL WARNING] Nie wykryto żadnych strumieni LSL.")
        return

    shimmer_stream = streams[0]
    inlet = StreamInlet(shimmer_stream)
    info = inlet.info()

    time_offset = time.time() - local_clock()
    print(f"[LSL] Połączono ze strumieniem: '{info.name()}' ({info.channel_count()} kanałów, {info.nominal_srate()} Hz)")

    ch_elem = info.desc().child("channels").child("channel")
    available_channels = {}

    for idx in range(info.channel_count()):
        label = ch_elem.child_value("label") or ch_elem.child_value("name") or f"Ch_{idx}"
        available_channels[label] = idx
        ch_elem = ch_elem.next_sibling()

    channel_indices = {}
    missing_channels = []

    for json_key, lsl_label in TARGET_CHANNELS.items():
        if lsl_label in available_channels:
            channel_indices[json_key] = (available_channels[lsl_label], lsl_label)
        else:
            missing_channels.append(lsl_label)

    if missing_channels:
        print(f"[LSL ERROR] Brak wymaganych kanałów w strumieniu: {missing_channels}")
        print(f"[LSL INFO] Dostępne kanały: {list(available_channels.keys())}")
        return

    print(f"[LSL] Rozpoczęto strumieniowanie '{sensor_name}' do Kafki ({TOPIC_NAME}) dla ID: {participation_id}...")

    count = 0
    try:
        while not stop_event.is_set():
            sample, ts_lsl = inlet.pull_sample(timeout=0.5)

            if sample is None:
                continue

            count += 1
            ts_ms = int((ts_lsl + time_offset) * 1000)

            payload = {
                "participation_id": participation_id,
                "ts": ts_ms,
                "hr": 75.0
            }

            for json_key, (idx, _) in channel_indices.items():
                payload[json_key] = float(sample[idx])

            producer.produce(
                TOPIC_NAME,
                json.dumps(payload).encode('utf-8'),
                callback=delivery_report
            )
            producer.poll(0)

            if count % 50 == 0:
                signals_str = " | ".join(f"{k}: {payload[k]:.2f}" for k in channel_indices.keys())
                print(f"[{info.name()} | {ts_ms}] Sent to Kafka | {signals_str}")

    except Exception as e:
        print(f"[LSL PIPELINE ERROR] {e}")
    finally:
        producer.flush()
        print("[LSL] Strumieniowanie biosygnałów zostało bezpiecznie zakończone.")