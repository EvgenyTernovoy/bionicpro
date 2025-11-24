from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.http.hooks.http import HttpHook
from airflow.providers.postgres.hooks.postgres import PostgresHook
from datetime import datetime, timedelta
import pandas as pd

default_args = {
    "owner": "etl",
    "retries": 0,
}

dag = DAG(
    "etl_crm_telemetry_mart",
    default_args=default_args,
    schedule_interval="0 */1 * * *",  # каждый час
    start_date=datetime(2024, 1, 1),
    catchup=False,
)


# ==========================================================
#                   1. EXTRACT CRM
# ==========================================================
def extract_crm():
    http = HttpHook(method="GET", http_conn_id="crm_api")
    response = http.run("/clients")
    clients = response.json()

    olap = PostgresHook(postgres_conn_id="olap")

    # Гарантируем таблицу
    olap.run(
        """
        CREATE TABLE IF NOT EXISTS staging.clients (
            id TEXT PRIMARY KEY,
            name TEXT,
            created_at TIMESTAMP
        );
    """
    )

    olap.run("TRUNCATE staging.clients;")

    insert_sql = """
        INSERT INTO staging.clients (id, name, created_at)
        VALUES (%s, %s, %s)
    """

    for c in clients:
        olap.run(insert_sql, parameters=(c["id"], c["name"], c["created_at"]))


# ==========================================================
#               2. EXTRACT TELEMETRY
# ==========================================================
def extract_telemetry():
    raw = PostgresHook(postgres_conn_id="raw_db")

    df = raw.get_pandas_df(
        """
        SELECT client_id, event_time, metric_value
        FROM telemetry
        WHERE event_time >= NOW() - INTERVAL '1 hour'
    """
    )

    olap = PostgresHook(postgres_conn_id="olap")

    olap.run(
        """
        CREATE TABLE IF NOT EXISTS staging.telemetry (
            client_id TEXT,
            event_time TIMESTAMP,
            metric_value DOUBLE PRECISION
        );
    """
    )

    for _, r in df.iterrows():
        olap.run(
            """
            INSERT INTO staging.telemetry (client_id, event_time, metric_value)
            VALUES (%s, %s, %s)
            """,
            parameters=(r.client_id, r.event_time, r.metric_value),
        )


# ==========================================================
#         3. TRANSFORM — AGGREGATE TELEMETRY
# ==========================================================
def aggregate_telemetry():
    olap = PostgresHook(postgres_conn_id="olap")

    olap.run(
        """
        CREATE TABLE IF NOT EXISTS olap.telemetry_agg (
            client_id TEXT PRIMARY KEY,
            events_total INT,
            metric_avg DOUBLE PRECISION,
            metric_max DOUBLE PRECISION,
            metric_min DOUBLE PRECISION,
            first_event TIMESTAMP,
            last_event TIMESTAMP
        );
    """
    )

    olap.run("TRUNCATE olap.telemetry_agg;")

    olap.run(
        """
        INSERT INTO olap.telemetry_agg 
        (client_id, events_total, metric_avg, metric_max, metric_min, first_event, last_event)
        SELECT 
            client_id,
            COUNT(*) AS events_total,
            AVG(metric_value),
            MAX(metric_value),
            MIN(metric_value),
            MIN(event_time),
            MAX(event_time)
        FROM staging.telemetry
        GROUP BY client_id;
    """
    )


# ==========================================================
#              4. BUILD MART (витрина)
# ==========================================================
def build_mart():
    olap = PostgresHook(postgres_conn_id="olap")

    olap.run(
        """
        CREATE TABLE IF NOT EXISTS mart.client_stats (
            client_id TEXT PRIMARY KEY,
            client_name TEXT,
            events_total INT,
            metric_avg DOUBLE PRECISION,
            metric_max DOUBLE PRECISION,
            metric_min DOUBLE PRECISION,
            first_event TIMESTAMP,
            last_event TIMESTAMP
        );
    """
    )

    olap.run("TRUNCATE mart.client_stats;")

    olap.run(
        """
        INSERT INTO mart.client_stats
        (client_id, client_name, events_total, metric_avg, metric_max, metric_min, first_event, last_event)
        SELECT 
            c.id,
            c.name,
            t.events_total,
            t.metric_avg,
            t.metric_max,
            t.metric_min,
            t.first_event,
            t.last_event
        FROM staging.clients c
        LEFT JOIN olap.telemetry_agg t 
        ON c.id = t.client_id;
    """
    )


# ==========================================================
#                     Airflow tasks
# ==========================================================
extract_crm_task = PythonOperator(
    task_id="extract_crm",
    python_callable=extract_crm,
    dag=dag,
)

extract_tel_task = PythonOperator(
    task_id="extract_telemetry",
    python_callable=extract_telemetry,
    dag=dag,
)

aggregate_task = PythonOperator(
    task_id="aggregate_telemetry",
    python_callable=aggregate_telemetry,
    dag=dag,
)

mart_task = PythonOperator(
    task_id="build_mart",
    python_callable=build_mart,
    dag=dag,
)

extract_crm_task >> extract_tel_task >> aggregate_task >> mart_task
