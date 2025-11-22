from fastapi import FastAPI, Request, HTTPException
import csv
import sys
import io
import asyncpg
from datetime import datetime
import httpx
import logging
import asyncio
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse

from .s3_client import MinioS3Client  # <-- твой клиент

s3 = MinioS3Client()
BUCKET = "reports"
CDN_BASE_URL = "http://localhost:8081/reports"  # nginx → minio
DATABASE_DSN = "postgresql://airflow:airflow@postgres_dag:5432/olap"
AUTH_SERVICE_URL = "http://bionicpro-auth:3010/api/protected"

app = FastAPI()

logging.basicConfig(
    level=logging.DEBUG,  # или INFO
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    handlers=[logging.StreamHandler(sys.stdout)],  # важно для Docker!
)
logger = logging.getLogger("report")

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


async def run_in_thread(func, *args, **kwargs):
    return await asyncio.to_thread(func, *args, **kwargs)


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
    logger.info(f"new_session: {new_session}")
    return user_id, new_session


@app.get("/reports")
async def download_report(request: Request):
    # 1. Аутентификация
    user_id, new_session = await get_authenticated_user(request)

    # 2. Генерируем ключ отчета
    report_type = "client_stats"

    current_minute = datetime.utcnow().strftime("%Y%m%d%H%M")

    params = {
        "client_id": user_id,
        "minute": current_minute,  # добавляем округлённое время
    }
    key = s3.build_report_key(
        report_type=report_type, client_id=str(user_id), params=params
    )

    # 3. Проверяем — может отчет уже в S3?
    exists = await run_in_thread(s3.exists, key, BUCKET)

    if exists:
        # Возвращаем ссылку CDN
        url = f"{CDN_BASE_URL}/{key}"

        resp = JSONResponse(content={"url": url})
        resp.headers["X-User-Email"] = user_id

        if new_session:
            resp.set_cookie(
                key="bionicpro_session",
                value=new_session,
                httponly=True,
                samesite="Lax",
                max_age=1800,
            )
        return resp

    # 4. Генерация отчета из БД
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

    # 5. Формируем CSV
    output = io.StringIO()
    writer = csv.writer(output)
    writer.writerow([col for col in rows[0].keys()] if rows else [])
    for r in rows:
        writer.writerow([r[col] for col in r.keys()])
    csv_data = output.getvalue()
    csv_bytes = csv_data.encode("utf-8")

    # 6. Загружаем CSV в S3 (multipart автоматически, если нужно)
    await run_in_thread(s3.upload_bytes, key, csv_bytes, BUCKET, "text/csv")

    # 7. Возвращаем ссылку CDN
    url = f"{CDN_BASE_URL}/{key}"

    resp = JSONResponse(content={"url": url})
    resp.headers["X-User-Email"] = user_id
    # 8. rotated session cookie
    if new_session:
        resp.set_cookie(
            key="bionicpro_session",
            value=new_session,
            httponly=True,
            samesite="Lax",
            max_age=1800,
        )

    return resp
