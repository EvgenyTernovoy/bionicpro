from fastapi import FastAPI, Response, Request, HTTPException
import csv
import io
import asyncpg
import httpx
import logging
from fastapi.middleware.cors import CORSMiddleware

app = FastAPI()

origins = [
    "http://localhost:3000",  # фронтенд
]

app.add_middleware(
    CORSMiddleware,
    allow_origins=origins,
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

logger = logging.getLogger("auth")
DATABASE_DSN = "postgresql://airflow:airflow@postgres_dag:5432/olap"
AUTH_SERVICE_URL = "http://bionicpro-auth:3010/api/protected"


async def get_authenticated_user(request: Request):
    session_cookie = request.cookies.get("bionicpro_session")
    if not session_cookie:
        raise HTTPException(status_code=401, detail="No session")

    async with httpx.AsyncClient() as client:
        resp = await client.get(
            AUTH_SERVICE_URL,
            headers={"Cookie": f"bionicpro_session={session_cookie}"},
            timeout=10.0,
        )

    logger.info(f"Responce: {resp}")

    if resp.status_code != 200:
        raise HTTPException(status_code=401, detail=f"Session invalid: {resp}")

    payload = resp.json()

    # ваш auth-сервис должен вернуть user_id в payload
    # если не возвращает — я покажу как добавить
    user_id = payload.get("user", {}).get("id") or payload.get("user_id")
    if not user_id:
        raise HTTPException(500, "Auth service did not return user_id")

    # актуализируем rotated session cookie
    new_session = payload.get("session_id")
    return user_id, new_session


@app.get("/reports")
async def download_report(request: Request):
    user_id, new_session = await get_authenticated_user(request)

    conn = await asyncpg.connect(DATABASE_DSN)

    rows = await conn.fetch(
        """
        SELECT *
        FROM mart.client_stats
        WHERE client_id = $1
    """,
        user_id,
    )

    await conn.close()

    # Формируем CSV в памяти
    output = io.StringIO()
    writer = csv.writer(output)
    writer.writerow([col for col in rows[0].keys()] if rows else [])
    for r in rows:
        writer.writerow([r[col] for col in r.keys()])

    csv_data = output.getvalue()

    resp = Response(
        content=csv_data,
        media_type="text/csv",
        headers={"Content-Disposition": f"attachment; filename=report_{user_id}.csv"},
    )

    # Обновляем rotated session cookie (реализация вашего auth-сервиса)
    if new_session:
        resp.set_cookie(
            key="bionicpro_session",
            value=new_session,
            httponly=True,
            samesite="Lax",
            max_age=1800,
        )

    return resp
