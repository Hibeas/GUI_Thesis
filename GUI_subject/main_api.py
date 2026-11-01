"""
Main FastAPI server for the Subject Machine.
Serves participant GUI, streams camera feed, handles experiment status,
and manages 'active' station state in PostgreSQL via Heartbeat.
"""
import threading
import os
import time
import sys
import webbrowser
import psycopg2
from contextlib import asynccontextmanager
from datetime import datetime
from fastapi import FastAPI
from fastapi.responses import StreamingResponse, HTMLResponse
from pydantic import BaseModel
from sshtunnel import SSHTunnelForwarder

import bronze_landmarks 
import lsl_to_kafka
import config as config

# Shared Thread Control Variables
camera_thread = None
lsl_thread = None
current_subject_id = "Brak"
current_experiment_id = "Brak"

ssh_tunnels = []
stop_heartbeat = threading.Event()
cleaned_up = False
cleanup_lock = threading.Lock()

class SessionConfig(BaseModel):
    subject_id: str
    experiment_id: str = "default_experiment"

# ==============================================================================
# --- PIPELINE CONTROLLER (CAMERA + LSL) ---
# ==============================================================================

def start_experiment_pipeline(session_dict: dict):
    """
    Uruchamia równolegle analizę wideo (MediaPipe) oraz zbiór biosygnałów LSL.
    """
    global camera_thread, lsl_thread
    
    # Reset flag zatrzymania
    bronze_landmarks.stop_event.clear()
    lsl_to_kafka.stop_event.clear()

    # 1. Start wątku kamery i MediaPipe
    camera_thread = threading.Thread(
        target=bronze_landmarks.run_landmarks, 
        args=(session_dict,), 
        daemon=True
    )
    camera_thread.start()

    # 2. Start wątku biosygnałów (LSL -> Kafka)
    lsl_thread = threading.Thread(
        target=lsl_to_kafka.run_lsl, 
        args=(session_dict,), 
        daemon=True
    )
    lsl_thread.start()

def stop_experiment_pipeline():
    """
    Bezpiecznie zatrzymuje oba strumienie danych.
    """
    global camera_thread, lsl_thread
    bronze_landmarks.stop_event.set()
    lsl_to_kafka.stop_event.set()
    
    if camera_thread and camera_thread.is_alive():
        camera_thread.join(timeout=2.0)
    if lsl_thread and lsl_thread.is_alive():
        lsl_thread.join(timeout=2.0)

# ==============================================================================
# --- SYSTEM & LIFECYCLE MANAGEMENT ---
# ==============================================================================

def start_ssh_tunnels():
    global ssh_tunnels
    print("Zestawianie tuneli SSH...")
    try:
        for local_port, remote_host, remote_port in config.SSH_PORTS_MAPPING:
            tunnel = SSHTunnelForwarder(
                (config.SSH_HOST, config.SSH_PORT),
                ssh_username=config.SSH_USER,
                ssh_password=config.SSH_PASSWORD,
                remote_bind_address=(remote_host, remote_port),
                local_bind_address=('127.0.0.1', local_port)
            )
            tunnel.start()
            ssh_tunnels.append(tunnel)
            print(f"[SSH] Tunel aktywny: localhost:{local_port} -> {remote_host}:{remote_port}")
        print("Wszystkie tunele SSH zostały pomyślnie uruchomione.")
    except Exception as e:
        print(f"[SSH ERROR] Nie udało się zestawić tunelu SSH: {e}")

def set_station_inactive():
    """Wymusza zmianę stacji na nieaktywną przy zamknięciu."""
    try:
        conn = psycopg2.connect(
            dbname=config.DB_NAME,
            user=config.DB_USER,
            password=config.DB_PASSWORD,
            host=config.DB_HOST,
            port=config.DB_PORT
        )
        cur = conn.cursor()
        cur.execute(
            'UPDATE "station" SET active = false WHERE station_code = %s;',
            (config.STATION_CODE,)
        )
        conn.commit()
        cur.close()
        conn.close()
        print(f"[DATABASE] Stanowisko {config.STATION_CODE} zostało wyłączone.")
    except Exception as e:
        print(f"[DATABASE ERROR] Błąd wyłączania stacji: {e}")

def cleanup_on_close():
    global cleaned_up
    with cleanup_lock:
        if cleaned_up:
            return
        cleaned_up = True

    print("\nZamykanie aplikacji Stanowiska...")
    stop_heartbeat.set()
    stop_experiment_pipeline()
    set_station_inactive()
    
    for tunnel in ssh_tunnels:
        try:
            tunnel.stop()
        except Exception:
            pass
    print("[SSH] Tunele SSH zostały zamknięte.")

@asynccontextmanager
async def lifespan(app: FastAPI):
    # --- STARTUP ---
    start_ssh_tunnels()
    
    # Uruchomienie pętli synchronicznej w tle
    heartbeat_thread = threading.Thread(target=heartbeat_loop, daemon=True)
    heartbeat_thread.start()
    print(f"[HEARTBEAT] Rozpoczęto wysyłanie sygnału aktywności dla stanowiska {config.STATION_CODE}")

    # Otwarcie przeglądarki
    threading.Thread(target=open_browser, daemon=True).start()

    yield

    # --- SHUTDOWN ---
    cleanup_on_close()

app = FastAPI(title="Participant GUI API", lifespan=lifespan)

# ==============================================================================
# --- POSTGRES HEARTBEAT & TASK SYNC MANAGEMENT ---
# ==============================================================================

def heartbeat_loop():
    """
    Wątek w tle:
    1. Aktualizuje timestamp last_seen dla stanowiska.
    2. Sprawdza w bazie PostgreSQL, czy stanowisko ma uruchomiony eksperyment.
    """
    global camera_thread, current_subject_id, current_experiment_id

    while not stop_heartbeat.is_set():
        try:
            conn = psycopg2.connect(
                dbname=config.DB_NAME,
                user=config.DB_USER,
                password=config.DB_PASSWORD,
                host=config.DB_HOST,
                port=config.DB_PORT
            )
            cur = conn.cursor()
            
            # 1. Odświeżenie Heartbeatu stanowiska
            cur.execute(
                'UPDATE "station" SET active = true, last_seen = %s WHERE station_code = %s;',
                (datetime.now(), config.STATION_CODE)
            )

            # 2. Sprawdzenie aktywnego zadania w tabeli participation
            query_task = '''
                SELECT p.participantid, s.experiment_type 
                FROM "participation" p
                JOIN "session" s ON p.sessionid = s.sessionid
                WHERE p.station_code = %s AND p.status = 'RUNNING'
                ORDER BY s.sessionid DESC LIMIT 1;
            '''
            cur.execute(query_task, (config.STATION_CODE,))
            task = cur.fetchone()

            conn.commit()
            cur.close()
            conn.close()

            # 3. Reakcja na stan z bazy danych
            if task:
                subject_id, exp_type = str(task[0]), str(task[1])
                if camera_thread is None or not camera_thread.is_alive():
                    print(f"[AUTO-START] Wykryto aktywny eksperyment dla uczestnika: {subject_id}")
                    current_subject_id = subject_id
                    current_experiment_id = exp_type
                    
                    session_dict = {"subject_id": subject_id, "experiment_id": exp_type}
                    start_experiment_pipeline(session_dict)
            else:
                if camera_thread and camera_thread.is_alive():
                    print("[AUTO-STOP] Eksperyment został zakończony w bazie. Zatrzymywanie procesów...")
                    stop_experiment_pipeline()

        except Exception as e:
            if not stop_heartbeat.is_set():
                print(f"[HEARTBEAT ERROR] Błąd w pętli synchronizacji: {e}")
        
        time.sleep(3)

# ==============================================================================
# --- FASTAPI ENDPOINTS ---
# ==============================================================================

@app.get("/", response_class=HTMLResponse)
def read_root():
    html_path = os.path.join(config.BASE_DIR, "index.html")
    if not os.path.exists(html_path):
        return HTMLResponse(content="<h1>Plik index.html nie został znaleziony.</h1>", status_code=404)
    with open(html_path, "r", encoding="utf-8") as f:
        return HTMLResponse(content=f.read())

def generate_frames():
    while True:
        with bronze_landmarks.frame_lock:
            if bronze_landmarks.latest_frame is not None:
                yield (b'--frame\r\n' b'Content-Type: image/jpeg\r\n\r\n' + bronze_landmarks.latest_frame + b'\r\n')
        time.sleep(0.04)

@app.get("/video_feed")
def video_feed():
    return StreamingResponse(generate_frames(), media_type="multipart/x-mixed-replace; boundary=frame")

@app.get("/status")
def get_status():
    is_running = camera_thread.is_alive() if camera_thread else False
    return {
        "station_code": config.STATION_CODE,
        "is_running": is_running,
        "subject_id": current_subject_id if is_running else "Oczekiwanie...",
        "experiment_id": current_experiment_id if is_running else "Brak"
    }

@app.post("/start")
def start_camera(session_config: SessionConfig):
    global current_subject_id, current_experiment_id
    
    current_subject_id = session_config.subject_id
    current_experiment_id = session_config.experiment_id
    
    if camera_thread is None or not camera_thread.is_alive():
        start_experiment_pipeline(session_config.model_dump())
        return {"status": "Started", "message": f"Rozpoczęto badanie dla {session_config.subject_id}"}
    return {"status": "Already running", "message": "Proces analizy już działa."}

@app.post("/stop")
def stop_camera():
    stop_experiment_pipeline()
    return {"status": "Stopped", "message": "Analiza oraz zbiór biosygnałów zostały zatrzymane."}

def open_browser():
    time.sleep(1.5)
    webbrowser.open(f"http://localhost:{config.FASTAPI_PORT}")

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host=config.FASTAPI_HOST, port=config.FASTAPI_PORT)