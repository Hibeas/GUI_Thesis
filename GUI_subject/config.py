"""
Configuration settings for the Subject Machine.
Loads sensitive credentials from .env file.
"""
import os
import sys
from dotenv import load_dotenv

def get_base_path() -> str:
    if getattr(sys, 'frozen', False):
        return getattr(sys, '_MEIPASS', os.path.dirname(sys.executable))
    return os.path.dirname(os.path.abspath(__file__))

def get_app_dir() -> str:
    if getattr(sys, 'frozen', False):
        return os.path.dirname(sys.executable)
    return os.path.dirname(os.path.abspath(__file__))

BASE_DIR = get_base_path()
load_dotenv(os.path.join(get_app_dir(), ".env"))

# --- Identification & Heartbeat ---
HEARTBEAT_TIMEOUT_SECONDS = 15
MAX_STATIONS = int(os.getenv("MAX_STATIONS", "8"))

# --- LSL Network Configuration ---
_peers_raw = os.getenv("LSL_KNOWN_PEERS", "153.19.52.45")
LSL_KNOWN_PEERS = [ip.strip() for ip in _peers_raw.split(",") if ip.strip()]

# --- Server & Network ---
FASTAPI_HOST = "0.0.0.0"
FASTAPI_PORT = 8001

# --- PostgreSQL Direct Connection ---
DB_NAME = os.getenv("DB_NAME", "experiment_db")
DB_USER = os.getenv("DB_USER", "admin")
DB_PASSWORD = os.getenv("DB_PASSWORD", "")
DB_HOST = os.getenv("DB_HOST", "localhost")
DB_PORT = int(os.getenv("DB_PORT", "5432"))

# --- SSH Tunneling Config ---
SSH_HOST = os.getenv("SSH_HOST", "")
SSH_PORT = int(os.getenv("SSH_PORT", "22"))
SSH_USER = os.getenv("SSH_USER", "")
SSH_PASSWORD = os.getenv("SSH_PASSWORD", "")

SSH_PORTS_MAPPING = [
    (DB_PORT, 'localhost', DB_PORT),
    (8081, 'localhost', 8081),
    (9005, 'localhost', 9005),
]

# --- Kafka & MinIO ---
KAFKA_SERVER = os.getenv("KAFKA_SERVER", "localhost:8081")
KAFKA_TOPIC_LANDMARKS = 'face-landmarks'
KAFKA_TOPIC_BIOSIGNALS = 'biosignal-data'

MINIO_URL = os.getenv("MINIO_URL", "http://localhost:9005")
MINIO_ACCESS_KEY = os.getenv("MINIO_ACCESS_KEY", "")
MINIO_SECRET_KEY = os.getenv("MINIO_SECRET_KEY", "")
BUCKET_NAME = os.getenv("BUCKET_NAME", "bronze")

# --- Video Processing ---
SEGMENT_DURATION = 20
MODEL_PATH = os.path.join(BASE_DIR, 'Models', 'face_landmarker.task')
TEMP_DIR = os.path.join(os.getcwd(), "temp_videos")