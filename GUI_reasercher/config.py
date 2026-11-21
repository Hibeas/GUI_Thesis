"""
Configuration settings for the Researcher Application.
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
    """Zwraca folder, w którym leży plik .exe lub skrypt (tam szukamy .env)."""
    if getattr(sys, 'frozen', False):
        return os.path.dirname(sys.executable)
    return os.path.dirname(os.path.abspath(__file__))

BASE_DIR = get_base_path()
load_dotenv(os.path.join(get_app_dir(), ".env"))

# --- Experiment Constraints ---
MAX_STATIONS = int(os.getenv("MAX_STATIONS", "8"))

# --- Database Connection Parameters ---
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
]

# --- API Server Settings ---
SERVER_HOST = "0.0.0.0"
SERVER_PORT = 8000