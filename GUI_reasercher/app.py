"""
FastAPI Backend for the Researcher GUI.
Handles session management, participant registration, active station monitoring,
and PostgreSQL metadata persistence.
"""
import os
import psycopg2
import uvicorn
from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import HTMLResponse
from pydantic import BaseModel
from typing import List, Optional
from datetime import datetime

import config

app = FastAPI(title="Researcher Control API")

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

class SessionStart(BaseModel):
    experiment_type: str
    location: str
    assignments: List[StationAssignment]

class SessionStop(BaseModel):
    session_id: int

# ==============================================================================
# --- DATABASE HELPER ---
# ==============================================================================

def get_db_connection():
    return psycopg2.connect(
        dbname=config.DB_NAME,
        user=config.DB_USER,
        password=config.DB_PASSWORD,
        host=config.DB_HOST,
        port=config.DB_PORT
    )

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
        conn = get_db_connection()
        cur = conn.cursor()
        cur.execute('SELECT participantid, name FROM "participant" ORDER BY name;')
        rows = cur.fetchall()
        participants = [{"id": str(row[0]), "name": row[1]} for row in rows]
        cur.close()
        conn.close()
        return participants
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Database error: {e}")

@app.post("/api/participants")
async def add_participant(participant: ParticipantCreate):
    try:
        conn = get_db_connection()
        cur = conn.cursor()
        query = 'INSERT INTO "participant" (name, birthday, gender) VALUES (%s, %s, %s);'
        cur.execute(query, (participant.name, participant.birthday, participant.gender))
        conn.commit()
        cur.close()
        conn.close()
        return {"status": "success", "message": "Participant added successfully"}
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to add participant: {e}")

@app.get("/api/sessions")
async def get_sessions():
    try:
        conn = get_db_connection()
        cur = conn.cursor()
        cur.execute('SELECT sessionid, experiment_type, location, start_time, end_time FROM "session" ORDER BY sessionid DESC;')
        rows = cur.fetchall()
        sessions = [
            {
                "sessionid": row[0],
                "experiment_type": row[1],
                "location": row[2],
                "start_time": str(row[3]) if row[3] else None,
                "end_time": str(row[4]) if row[4] else None
            } for row in rows
        ]
        cur.close()
        conn.close()
        return sessions
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Database error: {e}")

# W pliku app.py:

@app.post("/api/sessions/start")
async def start_session(session: SessionStart):
    try:
        conn = get_db_connection()
        cur = conn.cursor()
        now = datetime.now()

        # 1. Tworzenie sesji
        query_session = '''
            INSERT INTO "session" (experiment_type, location, start_time) 
            VALUES (%s, %s, %s) RETURNING sessionid;
        '''
        cur.execute(query_session, (session.experiment_type, session.location, now))
        session_id = cur.fetchone()[0]

        # 2. Zapis przypisań ze statusem RUNNING
        for assign in session.assignments:
            part_id = f"part_{assign.participant_id}_sess_{session_id}_st_{assign.station_code}"
            query_part = '''
                INSERT INTO "participation" (participationid, participantid, sessionid, station_code, status)
                VALUES (%s, %s, %s, %s, 'RUNNING');
            '''
            cur.execute(query_part, (part_id, assign.participant_id, session_id, assign.station_code))

        conn.commit()
        cur.close()
        conn.close()

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
        conn = get_db_connection()
        cur = conn.cursor()
        now = datetime.now()

        # Zamykamy sesję w tabeli session
        query_sess = 'UPDATE "session" SET end_time = %s WHERE sessionid = %s;'
        cur.execute(query_sess, (now, payload.session_id))
        
        # Zmieniamy status w participation na STOPPED dla tej sesji
        query_part = 'UPDATE "participation" SET status = %s WHERE sessionid = %s;'
        cur.execute(query_part, ('STOPPED', payload.session_id))

        conn.commit()
        cur.close()
        conn.close()

        return {
            "status": "success", 
            "end_time": now.strftime('%Y-%m-%d %H:%M:%S')
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to stop session: {e}")
    
@app.get("/api/stations")
async def get_stations():
    """
    Zwraca tylko stacje, których last_seen jest nie starsze niż 10 sekund.
    """
    try:
        conn = get_db_connection()
        cur = conn.cursor()
        query = '''
            SELECT station_code 
            FROM "station" 
            WHERE active = true 
              AND last_seen >= NOW() - INTERVAL '10 seconds'
            ORDER BY station_code;
        '''
        cur.execute(query)
        rows = cur.fetchall()
        stations = [{"station_code": row[0]} for row in rows]
        cur.close()
        conn.close()
        return stations
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Database error: {e}")

if __name__ == "__main__":
    uvicorn.run(app, host=config.SERVER_HOST, port=config.SERVER_PORT)