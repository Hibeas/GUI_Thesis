"""
Configuration settings for the Researcher Application.
Centralizes database connections, host parameters, and file path routing.
"""
import os
import sys

def get_base_path() -> str:
    """
    Dynamically determines the absolute base path whether running as a script or PyInstaller .exe.
    
    Returns:
        str: Absolute base directory path.
    """
    if getattr(sys, 'frozen', False):
        return getattr(sys, '_MEIPASS', os.path.dirname(sys.executable))
    return os.path.dirname(os.path.abspath(__file__))

# --- Base Directory ---
BASE_DIR = get_base_path()

# --- Database Connection Parameters ---
DB_NAME = "experiment_db"
DB_USER = "admin"
DB_PASSWORD = "password123"
DB_HOST = "localhost"
DB_PORT = "5432"

# --- API Server Settings ---
SERVER_HOST = "0.0.0.0"
SERVER_PORT = 8000