"""
Configuration settings for the Subject Machine.
Contains network, server, station, LSL, and model parameters.
"""
import os
import sys

def get_base_path() -> str:
    if getattr(sys, 'frozen', False):
        return getattr(sys, '_MEIPASS', os.path.dirname(sys.executable))
    return os.path.dirname(os.path.abspath(__file__))

# --- Identification & Heartbeat ---
HEARTBEAT_TIMEOUT_SECONDS = 15  # Po ilu sekundach braku sygnału stacja uznawana jest za wolną
MAX_STATIONS = 8                # Maksymalna liczba obsługiwanych stanowisk (s1..s8)

# --- LSL Network Configuration ---
LSL_KNOWN_PEERS = ["153.19.52.45"]  # Adresy IP maszyn udostępniających strumienie LSL (Shimmer)

# --- Server & Network ---
FASTAPI_HOST = "0.0.0.0"
FASTAPI_PORT = 8001

# --- PostgreSQL Direct Connection ---
DB_NAME = "experiment_db"
DB_USER = "admin"
DB_PASSWORD = "password123"
DB_HOST = "localhost"
DB_PORT = 5432

# --- SSH Tunneling Config ---
SSH_HOST = "153.19.52.9"
SSH_PORT = 22
SSH_USER = "sskarbek"
SSH_PASSWORD = "Szymon2211"

# Tylko porty realnie używane przez stanowisko: (lokalny_port, zdalny_host, zdalny_port)
SSH_PORTS_MAPPING = [
    (5432, 'localhost', 5432),  # PostgreSQL
    (8081, 'localhost', 8081),  # Kafka Broker
    (9005, 'localhost', 9005),  # MinIO Storage
]

# --- Kafka & MinIO ---
KAFKA_SERVER = 'localhost:8081'
KAFKA_TOPIC_LANDMARKS = 'face-landmarks'
KAFKA_TOPIC_BIOSIGNALS = 'biosignal-data'

MINIO_URL = 'http://localhost:9005'
MINIO_ACCESS_KEY = 'admin'
MINIO_SECRET_KEY = 'password123'
BUCKET_NAME = "bronze"

# --- Video Processing ---
SEGMENT_DURATION = 20  # seconds
BASE_DIR = get_base_path()
MODEL_PATH = os.path.join(BASE_DIR, 'Models', 'face_landmarker.task')
TEMP_DIR = os.path.join(os.getcwd(), "temp_videos")