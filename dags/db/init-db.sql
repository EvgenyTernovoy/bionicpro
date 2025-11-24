-- ============================================================================
-- 1. Создаём базы данных
-- ============================================================================

CREATE DATABASE olap;
CREATE DATABASE telemetry;
CREATE DATABASE crm;

-- ============================================================================
-- 2. Создаём схемы и таблицы в БД "olap"
-- ============================================================================

\connect olap;

CREATE SCHEMA IF NOT EXISTS staging;
CREATE SCHEMA IF NOT EXISTS olap;
CREATE SCHEMA IF NOT EXISTS mart;

-- Таблица сырых данных телеметрии
CREATE TABLE IF NOT EXISTS staging.telemetry_raw (
    id SERIAL PRIMARY KEY,
    device_id TEXT NOT NULL,
    ts TIMESTAMP NOT NULL,
    temperature NUMERIC,
    voltage NUMERIC
);

-- Аггрегации телеметрии
CREATE TABLE IF NOT EXISTS olap.telemetry_agg (
    client_id TEXT PRIMARY KEY,
    events_total INT,
    metric_avg NUMERIC,
    metric_max NUMERIC,
    metric_min NUMERIC,
    first_event TIMESTAMP,
    last_event TIMESTAMP
);


-- Пример витрины
CREATE TABLE IF NOT EXISTS mart.telemetry_daily (
    device_id TEXT NOT NULL,
    date DATE NOT NULL,
    metric JSONB
);

-- ============================================================================
-- 3. Создаём схемы и таблицы в БД "telemetry"
-- ============================================================================

\connect telemetry;

CREATE SCHEMA IF NOT EXISTS public;

CREATE TABLE IF NOT EXISTS device_metrics (
    id SERIAL PRIMARY KEY,
    device_id TEXT NOT NULL,
    ts TIMESTAMP NOT NULL,
    payload JSONB
);

-- ============================================================================
-- 4. Создаём схемы и таблицы в БД "crm"
-- ============================================================================

\connect crm;

CREATE SCHEMA IF NOT EXISTS public;

CREATE TABLE IF NOT EXISTS customers (
    id TEXT PRIMARY KEY,
    name TEXT NOT NULL,
    email TEXT NOT NULL,
    created_at TIMESTAMP DEFAULT NOW()
);