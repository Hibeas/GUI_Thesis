import psycopg2
from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel

app = FastAPI()

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# --- MODELE DANYCH ---
class SessionCreate(BaseModel):
    experiment_type: str
    session_date: str
    location: str

class AssignParticipantPayload(BaseModel):
    station_code: str
    participant_id: str
    session_id: int

def get_db_connection():
    return psycopg2.connect(
        dbname="experiment_db", 
        user="admin",
        password="password123",
        host="localhost",
        port="5432"
    )

# --- POBIERANIE UCZESTNIKÓW DO DROPDOWNU ---
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
        raise HTTPException(status_code=500, detail=str(e))

# --- ENDPOINTY DLA SESJI ---
@app.get("/api/sessions")
async def get_sessions():
    try:
        conn = get_db_connection()
        cur = conn.cursor()
        cur.execute('SELECT sessionid, experiment_type, session_date, location FROM "session" ORDER BY sessionid DESC;')
        rows = cur.fetchall()
        sessions = [{
            "sessionid": row[0],
            "experiment_type": row[1],
            "session_date": str(row[2]),
            "location": row[3]
        } for row in rows]
        cur.close()
        conn.close()
        return sessions
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.post("/api/sessions")
async def create_session(session: SessionCreate):
    try:
        conn = get_db_connection()
        cur = conn.cursor()
        query = 'INSERT INTO "session" (experiment_type, session_date, location) VALUES (%s, %s, %s);'
        cur.execute(query, (session.experiment_type, session.session_date, session.location))
        conn.commit()
        cur.close()
        conn.close()
        return {"status": "success"}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

# --- POBIERANIE AKTYWNYCH STACJI ---
@app.get("/api/stations")
async def get_stations():
    try:
        conn = get_db_connection()
        cur = conn.cursor()
        # Wyciągamy stacje, które mają active = true (czyli te odpalone przez drugi program)
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
        raise HTTPException(status_code=500, detail=str(e))

# --- ZAPIS UCZESTNIKA DO AKTYWNEGO ELEMENTU HISTORII ---
@app.post("/api/stations/assign")
async def assign_participant(payload: AssignParticipantPayload):
    try:
        conn = get_db_connection()
        cur = conn.cursor()

        if not payload.participant_id:
            # Jeśli "Brak", czyścimy pole w aktualnie aktywnym rekordzie danej stacji
            cur.execute('UPDATE "station" SET "participation_id" = NULL WHERE "station_code" = %s AND active = true;', (payload.station_code,))
        else:
            # Sprawdzamy czy relacja w participation już istnieje
            cur.execute(
                'SELECT participationid FROM "participation" WHERE participantid = %s AND sessionid = %s;',
                (payload.participant_id, payload.session_id)
            )
            result = cur.fetchone()
            
            if result:
                part_id = result[0]
            else:
                part_id = f"part_{payload.participant_id}_sess_{payload.session_id}"
                cur.execute(
                    'INSERT INTO "participation" (participationid, participantid, sessionid) VALUES (%s, %s, %s);',
                    (part_id, payload.participant_id, payload.session_id)
                )
            
            # KLUCZOWE: Aktualizujemy participation_id tylko tam, gdzie active = true
            cur.execute(
                'UPDATE "station" SET "participation_id" = %s WHERE "station_code" = %s AND active = true;', 
                (part_id, payload.station_code)
            )

        conn.commit()
        cur.close()
        conn.close()
        return {"status": "success"}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)