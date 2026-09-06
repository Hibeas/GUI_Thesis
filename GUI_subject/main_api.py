"""
Main FastAPI server for the Subject Machine.
Handles SSH tunneling, serves the local UI, and receives commands from the Researcher GUI.
"""
import threading
import sys
import os
import time
import subprocess
import atexit
import webbrowser
import uvicorn
from fastapi import FastAPI
from fastapi.responses import StreamingResponse, HTMLResponse
from pydantic import BaseModel

import bronze_landmarks 
import config

app = FastAPI()
camera_thread = None
current_subject_id = "Brak"
ssh_process = None

class SessionConfig(BaseModel):
    """Pydantic model representing the payload sent from the Researcher GUI."""
    subject_id: str
    experiment_id: str = "default_experiment"

@app.get("/")
def read_root():
    """Endpoint serving the local Subject control panel (index.html)."""
    html_path = os.path.join(config.BASE_DIR, "index.html")
    with open(html_path, "r", encoding="utf-8") as f:
        return HTMLResponse(content=f.read())

def generate_frames():
    """Yields JPEG frames from the shared camera lock for streaming."""
    while True:
        with bronze_landmarks.frame_lock:
            if bronze_landmarks.latest_frame is not None:
                yield (b'--frame\r\n' b'Content-Type: image/jpeg\r\n\r\n' + bronze_landmarks.latest_frame + b'\r\n')
        time.sleep(0.03)

@app.get("/video_feed")
def video_feed():
    """Streams the processed video feed to the local UI or Researcher GUI."""
    return StreamingResponse(generate_frames(), media_type="multipart/x-mixed-replace; boundary=frame")

@app.post("/start")
def start_camera(session_config: SessionConfig):
    global camera_thread, current_subject_id
    
    current_subject_id = session_config.subject_id # Save the ID for the UI
    
    if camera_thread is None or not camera_thread.is_alive():
        bronze_landmarks.stop_event.clear() 
        camera_thread = threading.Thread(
            target=bronze_landmarks.run_landmarks, 
            args=(session_config.model_dump(),), 
            daemon=True
        )
        camera_thread.start()
        return {"status": "Camera started", "message": f"Session started for {session_config.subject_id}"}
    return {"status": "Already running", "message": "Proces już działa"}

@app.get("/status")
def get_status():
    """Returns the current session status to the local Subject UI."""
    is_running = camera_thread.is_alive() if camera_thread else False
    return {
        "is_running": is_running,
        "subject_id": current_subject_id if is_running else "Oczekuje..."
    }

@app.post("/stop")
def stop_camera():
    """Stops the camera pipeline safely."""
    global camera_thread
    if camera_thread and camera_thread.is_alive():
        bronze_landmarks.stop_event.set() 
        camera_thread.join(timeout=2.0)
        return {"status": "Stopped", "message": "Kamera i analiza zatrzymana."}
    return {"status": "Not running", "message": "Kamera aktualnie nie działa."}

def start_ssh_tunnel():
    """Establishes an SSH tunnel in the background for Kafka and MinIO communication."""
    global ssh_process
    print("Zestawianie tunelu SSH...")
    
    ssh_command = ["ssh", "-N"] + config.SSH_PORT_MAPPINGS + [config.SSH_USER_HOST]
    
    try:
        ssh_process = subprocess.Popen(ssh_command)
        print("Tunel SSH został uruchomiony.")
    except Exception as e:
        print(f"Błąd podczas uruchamiania tunelu SSH: {e}")

def stop_ssh_tunnel():
    """Terminates the SSH tunnel process when the app closes."""
    if ssh_process:
        ssh_process.terminate()
        print("Tunel SSH został zamknięty.")

atexit.register(stop_ssh_tunnel)

def open_browser():
    """Opens the default web browser to the FastAPI interface."""
    time.sleep(1.5)  # Wait briefly to ensure Uvicorn is fully started
    webbrowser.open(f"http://localhost:{config.FASTAPI_PORT}")

if __name__ == "__main__":
    start_ssh_tunnel()
    
    # Start the browser automatically in a background thread
    threading.Thread(target=open_browser, daemon=True).start()
    
    uvicorn.run(app, host=config.FASTAPI_HOST, port=config.FASTAPI_PORT)