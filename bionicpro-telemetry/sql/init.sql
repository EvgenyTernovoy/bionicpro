CREATE TABLE telemetry (
    id SERIAL PRIMARY KEY,
    client_id TEXT NOT NULL,
    event_time TIMESTAMP NOT NULL,
    metric_type TEXT,
    metric_value NUMERIC,
    payload JSONB
);

CREATE TABLE clients (
    id TEXT PRIMARY KEY,
    name TEXT,
    created_at TIMESTAMP
);
