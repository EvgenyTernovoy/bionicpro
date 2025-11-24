from fastapi import FastAPI

app = FastAPI()

fake_clients = [
    {"id": "prothetic1@example.com", "name": "Alice", "created_at": "2024-01-01"},
    {"id": "prothetic2@example.com", "name": "Bob", "created_at": "2024-01-05"},
    {"id": "prothetic3@example.com", "name": "Charlie", "created_at": "2024-01-10"},
]


@app.get("/clients")
def get_clients():
    return fake_clients
