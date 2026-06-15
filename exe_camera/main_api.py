import threading
import sys
import os
import time
import uvicorn
from fastapi import FastAPI
from fastapi.responses import StreamingResponse, HTMLResponse
import subprocess # <--- DODAJ TEN IMPORT NA GÓRZE PLIKU
import atexit     # <--- DODAJ TEN IMPORT (pomoże zamknąć tunel na koniec)

# Poprawna nazwa pliku bez dopisku "_1"
import Bronze_landmarks 

app = FastAPI()
camera_thread = None

def get_base_path():
    """Funkcja pozwalająca na odczytanie ścieżki po spakowaniu przez PyInstaller"""
    if getattr(sys, 'frozen', False):
        return sys._MEIPASS
    return os.path.dirname(os.path.abspath(__file__))

@app.get("/")
def read_root():
    """Endpoint serwujący Twój panel sterowania (index.html)"""
    base_path = get_base_path()
    html_path = os.path.join(base_path, "index.html")
    with open(html_path, "r", encoding="utf-8") as f:
        return HTMLResponse(content=f.read())

def generate_frames():
    while True:
        # Odniesienie do zmiennych globalnych wewnątrz modułu Bronze_landmarks
        with Bronze_landmarks.frame_lock:
            if Bronze_landmarks.latest_frame is not None:
                yield (b'--frame\r\n' b'Content-Type: image/jpeg\r\n\r\n' + Bronze_landmarks.latest_frame + b'\r\n')
        time.sleep(0.03)

@app.get("/video_feed")
def video_feed():
    return StreamingResponse(generate_frames(), media_type="multipart/x-mixed-replace; boundary=frame")

@app.post("/start")
def start_camera():
    global camera_thread
    if camera_thread is None or not camera_thread.is_alive():
        Bronze_landmarks.stop_event.clear() # Opuść flagę, żeby kamera mogła wystartować
        camera_thread = threading.Thread(target=Bronze_landmarks.run_landmarks, daemon=True)
        camera_thread.start()
        return {"status": "Camera started", "message": "Kamera i analiza uruchomiona"}
    return {"status": "Already running", "message": "Proces już działa"}

@app.post("/stop")
def stop_camera():
    global camera_thread
    if camera_thread and camera_thread.is_alive():
        Bronze_landmarks.stop_event.set() # Podnieś flagę - wątek sam się zamknie
        camera_thread.join(timeout=2.0)
        return {"status": "Stopped", "message": "Kamera i analiza zatrzymana."}
    return {"status": "Not running", "message": "Kamera aktualnie nie działa."}

@app.post("/repair")
def repair_camera():
    global camera_thread
    # 1. Zatrzymanie obecnego procesu (jeśli działa)
    if camera_thread and camera_thread.is_alive():
        Bronze_landmarks.stop_event.set()
        camera_thread.join(timeout=3.0)
        
    # 2. Ponowny start
    Bronze_landmarks.stop_event.clear()
    camera_thread = threading.Thread(target=Bronze_landmarks.run_landmarks, daemon=True)
    camera_thread.start()
    return {"status": "Repaired", "message": "Pomyślnie zrestartowano procesy."}



ssh_process = None

def start_ssh_tunnel():
    global ssh_process
    print("Zestawianie tunelu SSH...")
    
    # Flaga -N oznacza "tylko przekazuj porty, nie otwieraj konsoli na serwerze docelowym"
    ssh_command = [
        "ssh", "-N",
        "-L", "8080:localhost:8080",
        "-L", "8081:localhost:8081",
        "-L", "9005:localhost:9005",
        "-L", "8085:localhost:8085",
        "sskarbek@153.19.52.9"
    ]
    
    try:
        # Popen uruchamia proces w tle, nie blokując reszty aplikacji
        ssh_process = subprocess.Popen(ssh_command)
        print("Tunel SSH został uruchomiony.")
    except Exception as e:
        print(f"Błąd podczas uruchamiania tunelu SSH: {e}")

def stop_ssh_tunnel():
    """Funkcja zamykająca tunel przy wyłączaniu aplikacji"""
    if ssh_process:
        ssh_process.terminate()
        print("Tunel SSH został zamknięty.")

# Rejestrujemy funkcję zamykającą, aby odpaliła się przy zamykaniu programu
atexit.register(stop_ssh_tunnel)



if __name__ == "__main__":
    # 1. Uruchom tunel SSH
    start_ssh_tunnel()
    
    # 2. Uruchom główny serwer uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)