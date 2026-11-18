"""
Configuration settings for the Researcher Application.
Centralizes database connections, SSH tunnel parameters, host parameters, and file path routing.
"""
import os
import sys

def get_base_path() -> str:
    """
    Dynamically determines the absolute base path whether running as a script or PyInstaller .exe.
    """
    if getattr(sys, 'frozen', False):
        return getattr(sys, '_MEIPASS', os.path.dirname(sys.executable))
    return os.path.dirname(os.path.abspath(__file__))

# --- Base Directory ---
BASE_DIR = get_base_path()

# --- Experiment Constraints ---
MAX_STATIONS = 8

# --- Database Connection Parameters ---
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

# Porty niezbędne dla GUI Badacza: (lokalny_port, zdalny_host, zdalny_port)
SSH_PORTS_MAPPING = [
    (5432, 'localhost', 5432),  # PostgreSQL
]

# --- API Server Settings ---
SERVER_HOST = "0.0.0.0"
SERVER_PORT = 8000