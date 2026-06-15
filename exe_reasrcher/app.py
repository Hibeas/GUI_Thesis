import psycopg2
from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel

app = FastAPI()

# Kluczowe dla bezpieczeństwa: pozwala przeglądarce na wysyłanie danych do Pythona
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


# Model danych, który przyjdzie z formularza HTML
class ParticipantData(BaseModel):
    name: str
    birthday: str  # Format: YYYY-MM-DD
    gender: str


def get_db_connection():
    return psycopg2.connect(
        dbname="experiment_db",  # Twoja poprawna baza danych!
        user="admin",
        password="password123",
        host="localhost",
        port="5432",
    )


@app.post("/add-participant")
async def add_participant(participant: ParticipantData):
    try:
        conn = get_db_connection()
        cur = conn.cursor()

        # Wykonujemy bezpieczny INSERT z cudzysłowami, dokładnie tak jak w Twoim działającym kodzie
        query = 'INSERT INTO "participant" (name, birthday, gender) VALUES (%s, %s, %s);'
        cur.execute(
            query, (participant.name, participant.birthday, participant.gender)
        )

        conn.commit()
        cur.close()
        conn.close()

        return {"status": "success", "message": "Uczestnik dodany pomyślnie!"}

    except Exception as e:
        raise HTTPException(
            status_code=500, detail=f"Błąd bazy danych: {str(e)}"
        )


if __name__ == "__main__":
    import uvicorn

    # Uruchomienie serwera na porcie 8000
    uvicorn.run(app, host="127.0.0.1", port=8000)