"""
Configuration settings for the Subject Machine.
Contains network, server, and model parameters.
"""
import os
import sys

def get_base_path() -> str:
    """
    Dynamically find the absolute path whether running as script or .exe.
    
    Returns:
        str: The absolute base path of the application.
    """
    if getattr(sys, 'frozen', False):
        return getattr(sys, '_MEIPASS', os.path.dirname(sys.executable))
    return os.path.dirname(os.path.abspath(__file__))

# --- Server & Network ---
FASTAPI_HOST = "0.0.0.0"
FASTAPI_PORT = 8000

# --- SSH Tunneling ---
SSH_USER_HOST = "sskarbek@153.19.52.9"
SSH_PORT_MAPPINGS = [
    "-L", "8080:localhost:8080",
    "-L", "8081:localhost:8081",
    "-L", "9005:localhost:9005",
    "-L", "8085:localhost:8085"
]

# --- Kafka ---
KAFKA_SERVER = 'localhost:8081'
KAFKA_TOPIC_LANDMARKS = 'face-landmarks'

# --- MinIO / S3 ---
MINIO_URL = 'http://localhost:9005'
MINIO_ACCESS_KEY = 'admin'
MINIO_SECRET_KEY = 'password123'
BUCKET_NAME = "bronze"

# --- Video Processing ---
SEGMENT_DURATION = 20  # seconds
BASE_DIR = get_base_path()
MODEL_PATH = os.path.join(BASE_DIR, 'Models', 'face_landmarker.task')
TEMP_DIR = os.path.join(os.getcwd(), "temp_videos")