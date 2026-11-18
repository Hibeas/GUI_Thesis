"""
FastAPI Backend for the Researcher GUI.
Handles session management, participant registration, active station monitoring,
SSH tunnel management, and resilient PostgreSQL metadata persistence.
"""
import os
import socket
import threading
import time
import webbrowser
from contextlib import asynccontextmanager, contextmanager
from datetime import datetime
from typing import List, Optional

import psycopg2
from psycopg2 import pool
from sshtunnel import SSHTunnelForwarder
import uvicorn
from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import HTMLResponse
from pydantic import BaseModel

import config
from datetime import datetime
from zoneinfo import ZoneInfo

WARSAW_TZ = ZoneInfo("Europe/Warsaw")
db_pool: Optional[pool.ThreadedConnectionPool] = None
ssh_tunnels: List[SSHTunnelForwarder] = []

# ==============================================================================
# --- SSH TUNNEL MANAGEMENT ---
# ==============================================================================

def is_port_in_use(port: int, host: str = "127.0.0.1") -> bool:
    """Sprawdza, czy lokalny port jest już nasłuchiwany (np. przez inny tunel SSH)."""
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        s.settimeout(0.5)
        return s.connect_ex((host, port)) == 0

def start_ssh_tunnels():
    global ssh_tunnels
    ports_mapping = getattr(config, 'SSH_PORTS_MAPPING', [(5432, 'localhost', 5432)])
    
    for local_port, remote_host, remote_port in ports_mapping:
        if is_port_in_use(local_port):
            print(f"[SSH INFO] Port localhost:{local_port} jest już zajęty (aktywny inny tunel/usługa). Pomijam tworzenie.")
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

def stop_ssh_tunnels():
    for tunnel in ssh_tunnels:
        try:
            tunnel.stop()
        except Exception:
            pass
    ssh_tunnels.clear()
    print("[SSH] Tunele SSH utworzone przez Panel Badacza zostały zamknięte.")

def open_browser():
    time.sleep(1.2)
    webbrowser.open(f"http://localhost:{config.SERVER_PORT}")

# ==============================================================================
# --- LIFESPAN ---
# ==============================================================================

@asynccontextmanager
async def lifespan(app: FastAPI):
    global db_pool
    # 1. Start tuneli SSH (tylko jeśli porty nie są już otwarte)
    start_ssh_tunnels()

    # 2. Inicjalizacja puli połączeń do PostgreSQL
    db_pool = pool.ThreadedConnectionPool(
        minconn=1,
        maxconn=5,
        dbname=config.DB_NAME,
        user=config.DB_USER,
        password=config.DB_PASSWORD,
        host=config.DB_HOST,
        port=config.DB_PORT,
        connect_timeout=5
    )

    # 3. Otwarcie przeglądarki
    threading.Thread(target=open_browser, daemon=True).start()

    yield

    # --- SHUTDOWN ---
    if db_pool:
        db_pool.closeall()
    stop_ssh_tunnels()

app = FastAPI(title="Researcher Control API", lifespan=lifespan)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# ==============================================================================
# --- DATA MODELS ---
# ==============================================================================

class ParticipantCreate(BaseModel):
    name: str
    birthday: str
    gender: str

class StationAssignment(BaseModel):
    station_code: str
    participant_id: str
    sensor_name: Optional[str] = None

class SessionStart(BaseModel):
    experiment_type: str
    location: str
    assignments: List[StationAssignment]

class SessionStop(BaseModel):
    session_id: int

# ==============================================================================
# --- RESILIENT DATABASE POOL CONTEXT MANAGER ---
# ==============================================================================

@contextmanager
def get_db_cursor(commit: bool = False):
    if db_pool is None:
        raise RuntimeError("Database pool is not initialized")
    
    conn = db_pool.getconn()
    broken_connection = False
    try:
        # Jeśli połączenie zostało zamknięte po stronie serwera/tunelu, pobieramy świeże
        if conn.closed != 0:
            db_pool.putconn(conn, close=True)
            conn = db_pool.getconn()

        with conn.cursor() as cur:
            yield cur
        if commit:
            conn.commit()
    except (psycopg2.OperationalError, psycopg2.InterfaceError):
        broken_connection = True
        raise
    except Exception:
        if conn.closed == 0:
            conn.rollback()
        raise
    finally:
        # Martwe połączenie zamykamy i usuwamy z puli (close=True), sprawne zwracamy
        db_pool.putconn(conn, close=broken_connection or (conn.closed != 0))

# ==============================================================================
# --- API ENDPOINTS ---
# ==============================================================================

@app.get("/", response_class=HTMLResponse)
async def read_root():
    html_path = os.path.join(config.BASE_DIR, "index.html")
    if not os.path.exists(html_path):
        raise HTTPException(status_code=404, detail="index.html dashboard file not found.")
    with open(html_path, "r", encoding="utf-8") as f:
        return HTMLResponse(content=f.read())

@app.get("/api/participants")
async def get_participants():
    try:
        with get_db_cursor() as cur:
            cur.execute('SELECT participantid, name FROM "participant" ORDER BY name;')
            rows = cur.fetchall()
            return [{"id": str(row[0]), "name": row[1]} for row in rows]
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Database error: {e}")

@app.post("/api/participants")
async def add_participant(participant: ParticipantCreate):
    try:
        with get_db_cursor(commit=True) as cur:
            query = 'INSERT INTO "participant" (name, birthday, gender) VALUES (%s, %s, %s);'
            cur.execute(query, (participant.name, participant.birthday, participant.gender))
        return {"status": "success", "message": "Participant added successfully"}
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to add participant: {e}")

@app.get("/api/sessions")
async def get_sessions():
    try:
        with get_db_cursor() as cur:
            cur.execute('''
                SELECT sessionid, experiment_type, location, start_time, end_time 
                FROM "session" 
                ORDER BY sessionid DESC 
                LIMIT 50;
            ''')
            rows = cur.fetchall()
            return [
                {
                    "sessionid": row[0],
                    "experiment_type": row[1],
                    "location": row[2],
                    "start_time": str(row[3]) if row[3] else None,
                    "end_time": str(row[4]) if row[4] else None
                } for row in rows
            ]
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Database error: {e}")

@app.post("/api/sessions/start")
async def start_session(session: SessionStart):
    max_stations = getattr(config, 'MAX_STATIONS', 8)
    if len(session.assignments) > max_stations:
        raise HTTPException(
            status_code=400, 
            detail=f"Exceeded maximum allowed stations ({max_stations})."
        )
    try:
        now = datetime.now(WARSAW_TZ).replace(tzinfo=None)
        with get_db_cursor(commit=True) as cur:
            query_session = '''
                INSERT INTO "session" (experiment_type, location, start_time) 
                VALUES (%s, %s, %s) RETURNING sessionid;
            '''
            cur.execute(query_session, (session.experiment_type, session.location, now))
            session_id = cur.fetchone()[0]

            for assign in session.assignments:
                part_id = f"part_{assign.participant_id}_sess_{session_id}_st_{assign.station_code}"
                query_part = '''
                    INSERT INTO "participation" (participationid, participantid, sessionid, station_code, sensor_name, status)
                    VALUES (%s, %s, %s, %s, %s, 'RUNNING');
                '''
                cur.execute(query_part, (part_id, assign.participant_id, session_id, assign.station_code, assign.sensor_name))

        return {
            "status": "success", 
            "session_id": session_id, 
            "start_time": now.strftime('%Y-%m-%d %H:%M:%S')
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to start session: {e}")

@app.post("/api/sessions/stop")
async def stop_session(payload: SessionStop):
    try:
        now = datetime.now(WARSAW_TZ).replace(tzinfo=None)
        with get_db_cursor(commit=True) as cur:
            cur.execute('UPDATE "session" SET end_time = %s WHERE sessionid = %s;', (now, payload.session_id))
            cur.execute('UPDATE "participation" SET status = %s WHERE sessionid = %s;', ('STOPPED', payload.session_id))

        return {
            "status": "success", 
            "end_time": now.strftime('%Y-%m-%d %H:%M:%S')
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to stop session: {e}")
    
@app.get("/api/stations")
async def get_stations():
    max_stations = getattr(config, 'MAX_STATIONS', 8)
    try:
        with get_db_cursor() as cur:
            query = '''
                SELECT station_code 
                FROM "station" 
                WHERE active = true 
                  AND last_seen >= NOW() - INTERVAL '15 seconds'
                ORDER BY NULLIF(REGEXP_REPLACE(station_code, '[^0-9]', '', 'g'), '')::INT ASC NULLS LAST,
                         station_code ASC
                LIMIT %s;
            '''
            cur.execute(query, (max_stations,))
            rows = cur.fetchall()
            return [{"station_code": row[0]} for row in rows]
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Database error: {e}")

if __name__ == "__main__":
    uvicorn.run(app, host=config.SERVER_HOST, port=config.SERVER_PORT)