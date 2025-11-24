import requests
import random
import time
from datetime import datetime

CRM_CLIENTS = [
    "prothetic1@example.com",
    "prothetic2@example.com",
    "prothetic3@example.com",
]
URL = "http://localhost:9100/telemetry"

while True:
    payload = {
        "client_id": random.choice(CRM_CLIENTS),
        "metric_type": "temperature",
        "metric_value": round(random.uniform(20, 90), 2),
        "event_time": datetime.utcnow().isoformat(),
        "payload": {"unit": "C"},
    }

    requests.post(URL, json=payload)
    print("Sent:", payload)

    time.sleep(1)  # каждую секунду
