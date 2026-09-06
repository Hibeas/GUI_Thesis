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

import config

app = FastAPI(title="Researcher Control API")

# Enable CORS for cross-origin requests
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
    """Payload model for registering a new study participant."""
    name: str
    birthday: str
    gender: str

class SessionCreate(BaseModel):
    """Payload model for creating a new experimental session."""
    experiment_type: str
    session_date: str
    location: str

class AssignParticipantPayload(BaseModel):
    """Payload model for assigning a participant and session to an active station."""
    station_code: str
    participant_id: str
    session_id: int

# ==============================================================================
# --- DATABASE HELPER ---
# ==============================================================================

def get_db_connection():
    """
    Establishes and returns a new connection to the PostgreSQL database.
    
    Returns:
        psycopg2.extensions.connection: Active database connection instance.
    """
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
    """Serves the Researcher control panel user interface (index.html)."""
    html_path = os.path.join(config.BASE_DIR, "index.html")
    if not os.path.exists(html_path):
        raise HTTPException(status_code=404, detail="index.html dashboard file not found.")
    with open(html_path, "r", encoding="utf-8") as f:
        return HTMLResponse(content=f.read())

@app.get("/api/participants")
async def get_participants():
    """
    Retrieves all registered participants ordered by name.
    
    Returns:
        list: A list of dicts containing participant ID and name.
    """
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
    """
    Inserts a new participant record into the database.
    
    Args:
        participant (ParticipantCreate): The participant details payload.
    """
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
    """
    Retrieves all experimental sessions ordered by most recent session ID.
    
    Returns:
        list: A list of dicts containing session details.
    """
    try:
        conn = get_db_connection()
        cur = conn.cursor()
        cur.execute('SELECT sessionid, experiment_type, session_date, location FROM "session" ORDER BY sessionid DESC;')
        rows = cur.fetchall()
        sessions = [
            {
                "sessionid": row[0],
                "experiment_type": row[1],
                "session_date": str(row[2]),
                "location": row[3]
            } for row in rows
        ]
        cur.close()
        conn.close()
        return sessions
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Database error: {e}")

@app.post("/api/sessions")
async def create_session(session: SessionCreate):
    """
    Creates a new experiment session in the database.
    
    Args:
        session (SessionCreate): Session payload metadata.
    """
    try:
        conn = get_db_connection()
        cur = conn.cursor()
        query = 'INSERT INTO "session" (experiment_type, session_date, location) VALUES (%s, %s, %s);'
        cur.execute(query, (session.experiment_type, session.session_date, session.location))
        conn.commit()
        cur.close()
        conn.close()
        return {"status": "success", "message": "Session created successfully"}
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to create session: {e}")

@app.get("/api/stations")
async def get_stations():
    """
    Retrieves all currently active physical hardware stations.
    
    Returns:
        list: Active stations and their currently assigned participant IDs.
    """
    try:
        conn = get_db_connection()
        cur = conn.cursor()
        query = """
            SELECT 
                s.station_code, 
                p.participantid
            FROM "station" s
            LEFT JOIN "participation" p ON s.participation_id = p.participationid
            WHERE s.active = true
            ORDER BY s.station_code;
        """
        cur.execute(query)
        rows = cur.fetchall()
        stations = [{"station_code": row[0], "participant_id": str(row[1]) if row[1] else ""} for row in rows]
        cur.close()
        conn.close()
        return stations
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Database error: {e}")

@app.post("/api/stations/assign")
async def assign_participant(payload: AssignParticipantPayload):
    """
    Generates a participation record and binds a participant to an active station.
    
    Args:
        payload (AssignParticipantPayload): Station assignment payload.
    """
    try:
        conn = get_db_connection()
        cur = conn.cursor()

        if not payload.participant_id:
            # If empty/unassigned, clear the participation field for this active station
            cur.execute(
                'UPDATE "station" SET "participation_id" = NULL WHERE "station_code" = %s AND active = true;', 
                (payload.station_code,)
            )
        else:
            # Check if participation entry already exists for this subject & session
            cur.execute(
                'SELECT participationid FROM "participation" WHERE participantid = %s AND sessionid = %s;',
                (payload.participant_id, payload.session_id)
            )
            result = cur.fetchone()
            
            if result:
                part_id = result[0]
            else:
                # Generate unique structured participation ID
                part_id = f"part_{payload.participant_id}_sess_{payload.session_id}"
                cur.execute(
                    'INSERT INTO "participation" (participationid, participantid, sessionid) VALUES (%s, %s, %s);',
                    (part_id, payload.participant_id, payload.session_id)
                )
            
            # Update participation ID on active station
            cur.execute(
                'UPDATE "station" SET "participation_id" = %s WHERE "station_code" = %s AND active = true;', 
                (part_id, payload.station_code)
            )

        conn.commit()
        cur.close()
        conn.close()
        return {"status": "success", "participation_id": part_id if payload.participant_id else None}
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Assignment failed: {e}")

if __name__ == "__main__":
    uvicorn.run(app, host=config.SERVER_HOST, port=config.SERVER_PORT)