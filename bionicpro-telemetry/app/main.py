from fastapi import FastAPI
from pydantic import BaseModel
from datetime import datetime
from sqlalchemy import create_engine, text
import os
import json
from typing import Optional

DATABASE_URL = os.getenv("DATABASE_URL")

engine = create_engine(DATABASE_URL)

app = FastAPI()


class TelemetryEvent(BaseModel):
    client_id: str
    metric_type: str = "default"
    metric_value: float
    event_time: datetime = datetime.utcnow()
    payload: Optional[dict] = None


@app.post("/telemetry")
def ingest(event: TelemetryEvent):
    data = event.model_dump()

    # serialize payload → JSON string (PostgreSQL will parse it if column is JSONB)
    if data["payload"] is not None:
        data["payload"] = json.dumps(data["payload"])

    with engine.begin() as conn:
        conn.execute(
            text(
                """
                INSERT INTO telemetry (client_id, metric_type, metric_value, event_time, payload)
                VALUES (:client_id, :metric_type, :metric_value, :event_time, :payload)
                """
            ),
            data,
        )

    return {"status": "ok"}
