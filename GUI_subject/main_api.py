"""
Main FastAPI server for the Subject Machine.
Serves participant GUI, streams camera feed, handles experiment status,
and manages 'active' station state in PostgreSQL via a persistent Heartbeat connection.
"""
import threading
import os
import socket
import time
import webbrowser
import psycopg2
from contextlib import asynccontextmanager
from fastapi import FastAPI
from fastapi.responses import StreamingResponse, HTMLResponse
from pydantic import BaseModel
from sshtunnel import SSHTunnelForwarder

import bronze_landmarks
import lsl_to_kafka
import config

# Shared Thread Control Variables
camera_thread = None
lsl_thread = None
assigned_station_code = None
is_shutting_down = False
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
    global camera_thread, lsl_thread

    bronze_landmarks.stop_event.clear()
    lsl_to_kafka.stop_event.clear()

    camera_thread = threading.Thread(
        target=bronze_landmarks.run_landmarks,
        args=(session_dict,),
        daemon=True
    )
    camera_thread.start()

    lsl_thread = threading.Thread(
        target=lsl_to_kafka.run_lsl,
        args=(session_dict,),
        daemon=True
    )
    lsl_thread.start()

def stop_experiment_pipeline():
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

def is_port_in_use(port: int, host: str = "127.0.0.1") -> bool:
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        s.settimeout(0.5)
        return s.connect_ex((host, port)) == 0

def start_ssh_tunnels():
    global ssh_tunnels
    print("Zestawianie tuneli SSH...")
    for local_port, remote_host, remote_port in config.SSH_PORTS_MAPPING:
        if is_port_in_use(local_port):
            print(f"[SSH INFO] Port localhost:{local_port} jest już zajęty (aktywny inny tunel). Pomijam tworzenie.")
            continue
        try:
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
        except Exception as e:
            print(f"[SSH ERROR] Nie udało się zestawić tunelu na porcie {local_port}: {e}")

def get_db_connection():
    return psycopg2.connect(
        dbname=config.DB_NAME,
        user=config.DB_USER,
        password=config.DB_PASSWORD,
        host=config.DB_HOST,
        port=config.DB_PORT,
        connect_timeout=5
    )

def claim_available_station() -> str:
    timeout_sec = getattr(config, 'HEARTBEAT_TIMEOUT_SECONDS', 15)
    max_stations = getattr(config, 'MAX_STATIONS', 8)

    conn = get_db_connection()
    cur = conn.cursor()

    query_find = '''
        SELECT station_code
        FROM "station"
        WHERE (
            active = false
            OR last_seen IS NULL
            OR last_seen < NOW() - (%s * INTERVAL '1 second')
        )
        AND NULLIF(REGEXP_REPLACE(station_code, '[^0-9]', '', 'g'), '')::INT <= %s
        ORDER BY NULLIF(REGEXP_REPLACE(station_code, '[^0-9]', '', 'g'), '')::INT ASC NULLS LAST,
                 station_code ASC
        LIMIT 1
        FOR UPDATE SKIP LOCKED;
    '''
    cur.execute(query_find, (timeout_sec, max_stations))
    row = cur.fetchone()

    if row:
        station_code = row[0]
        cur.execute(
            'UPDATE "station" SET active = true, last_seen = NOW() WHERE station_code = %s;',
            (station_code,)
        )
    else:
        cur.execute('''
            SELECT COALESCE(MAX(NULLIF(REGEXP_REPLACE(station_code, '[^0-9]', '', 'g'), '')::INT), 0) + 1
            FROM "station";
        ''')
        next_num = cur.fetchone()[0]

        if next_num > max_stations:
            cur.close()
            conn.close()
            raise RuntimeError(
                f"Osiągnięto maksymalny limit aktywnych stanowisk ({max_stations}). "
                f"Zwolnij jedno ze stanowisk lub zwiększ MAX_STATIONS w config.py."
            )

        station_code = f"s{next_num}"
        cur.execute(
            'INSERT INTO "station" (station_code, active, last_seen) VALUES (%s, true, NOW());',
            (station_code,)
        )

    conn.commit()
    cur.close()
    conn.close()
    return station_code

def set_station_inactive():
    if not assigned_station_code:
        return
    try:
        conn = get_db_connection()
        cur = conn.cursor()
        cur.execute(
            'UPDATE "station" SET active = false WHERE station_code = %s;',
            (assigned_station_code,)
        )
        conn.commit()
        cur.close()
        conn.close()
        print(f"[DATABASE] Stanowisko {assigned_station_code} zostało wyłączone.")
    except Exception as e:
        print(f"[DATABASE ERROR] Błąd wyłączania stacji: {e}")

def cleanup_on_close():
    global cleaned_up, is_shutting_down
    is_shutting_down = True
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
    ssh_tunnels.clear()
    print("[SSH] Tunele SSH zostały zamknięte.")

@asynccontextmanager
async def lifespan(app: FastAPI):
    global assigned_station_code
    start_ssh_tunnels()

    assigned_station_code = claim_available_station()
    print(f"[STATION] Przydzielono wolne stanowisko: {assigned_station_code}")

    heartbeat_thread = threading.Thread(target=heartbeat_loop, daemon=True)
    heartbeat_thread.start()
    print(f"[HEARTBEAT] Rozpoczęto wysyłanie sygnału aktywności dla stanowiska {assigned_station_code}")

    # Uruchomienie workera wysyłającego nagrania do MinIO po zakończeniu sesji
    threading.Thread(target=minio_upload_worker, daemon=True).start()

    threading.Thread(target=open_browser, daemon=True).start()

    yield

    cleanup_on_close()
app = FastAPI(title="Participant GUI API", lifespan=lifespan)

# ==============================================================================
# --- PERSISTENT POSTGRES HEARTBEAT & TASK SYNC ---
# ==============================================================================

def heartbeat_loop():
    """
    Utrzymuje jedno stałe połączenie do PostgreSQL zamiast otwierać nowe co 3 sekundy.
    W razie zerwania tunelu automatycznie nawiązuje je ponownie.
    """
    global camera_thread, current_subject_id, current_experiment_id
    conn = None

    query_task = '''
        SELECT p.participantid, s.experiment_type, p.sensor_name, p.participationid
        FROM "participation" p
        JOIN "session" s ON p.sessionid = s.sessionid
        WHERE p.station_code = %s AND p.status = 'RUNNING'
        ORDER BY s.sessionid DESC LIMIT 1;
    '''

    while not stop_heartbeat.is_set():
        try:
            if conn is None or conn.closed != 0:
                conn = get_db_connection()

            with conn.cursor() as cur:
                cur.execute(
                    'UPDATE "station" SET active = true, last_seen = NOW() WHERE station_code = %s;',
                    (assigned_station_code,)
                )
                cur.execute(query_task, (assigned_station_code,))
                task = cur.fetchone()

            conn.commit()

            if task:
                subject_id = str(task[0])
                exp_type = str(task[1])
                sensor_name = str(task[2]) if task[2] else None
                participation_id = str(task[3])

                if camera_thread is None or not camera_thread.is_alive():
                    print(f"[AUTO-START] Wykryto eksperyment | Uczestnik: {subject_id} | Czujnik: {sensor_name}")
                    current_subject_id = subject_id
                    current_experiment_id = exp_type

                    session_dict = {
                        "subject_id": subject_id,
                        "experiment_id": exp_type,
                        "sensor_name": sensor_name,
                        "participation_id": participation_id
                    }
                    start_experiment_pipeline(session_dict)
            else:
                if camera_thread and camera_thread.is_alive():
                    print("[AUTO-STOP] Eksperyment został zakończony w bazie. Zatrzymywanie procesów...")
                    stop_experiment_pipeline()

        except Exception as e:
            if not stop_heartbeat.is_set():
                print(f"[HEARTBEAT ERROR] Błąd synchronizacji (ponawianie połączenia): {e}")
            if conn is not None:
                try:
                    conn.close()
                except Exception:
                    pass
                conn = None

        stop_heartbeat.wait(3.0)

    if conn is not None:
        try:
            conn.close()
        except Exception:
            pass

# ==============================================================================
# --- FASTAPI ENDPOINTS ---
# ==============================================================================
def minio_upload_worker():
    """Wysyła zgromadzone pliki wideo do MinIO wyłącznie w przerwach między eksperymentami."""
    while not is_shutting_down:
        is_recording = camera_thread is not None and camera_thread.is_alive()
        if not is_recording:
            bronze_landmarks.process_pending_uploads(
                lambda: camera_thread is not None and camera_thread.is_alive()
            )
        time.sleep(5.0)

@app.get("/", response_class=HTMLResponse)
def read_root():
    html_path = os.path.join(config.BASE_DIR, "index.html")
    if not os.path.exists(html_path):
        return HTMLResponse(content="<h1>Plik index.html nie został znaleziony.</h1>", status_code=404)
    with open(html_path, "r", encoding="utf-8") as f:
        return HTMLResponse(content=f.read())

def generate_frames():
    while not is_shutting_down and not stop_heartbeat.is_set():
        with bronze_landmarks.frame_lock:
            if bronze_landmarks.latest_frame is not None:
                yield (b'--frame\r\n' b'Content-Type: image/jpeg\r\n\r\n' + bronze_landmarks.latest_frame + b'\r\n')
        time.sleep(0.05)

@app.get("/video_feed")
def video_feed():
    return StreamingResponse(generate_frames(), media_type="multipart/x-mixed-replace; boundary=frame")

@app.get("/status")
def get_status():
    is_running = camera_thread.is_alive() if camera_thread else False
    return {
        "station_code": assigned_station_code or "Przydzielanie...",
        "is_running": is_running,
        "subject_id": current_subject_id if is_running else "Oczekiwanie...",
        "experiment_id": current_experiment_id if is_running else "Brak",
        "upload_status": bronze_landmarks.upload_status
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