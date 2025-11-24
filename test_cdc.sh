#!/bin/bash
set -euo pipefail

# ----------------------------------------
#            CONFIGURATION
# ----------------------------------------

# CRM DB
CRM_PG_CONTAINER="crm-db"
CRM_PG_USER="crm_user"
CRM_PG_DB="crm"
CRM_PG_PASSWORD="crm_password"
CRM_KAFKA_TOPIC="crm.public.customers"

# Telemetry DB
TEL_PG_CONTAINER="telemetry_db"
TEL_PG_USER="airflow"
TEL_PG_DB="telemetry"
TEL_PG_PASSWORD="airflow"
TEL_KAFKA_TOPIC="telemetry.public.telemetry"

# Kafka
KAFKA_CONTAINER="kafka"

# ClickHouse
CH_CONTAINER="clickhouse"
CH_DB="default"

# CRM ClickHouse tables
CRM_STAGING_TABLE="crm_customers_staging"
CRM_FINAL_TABLE="mv_crm_customers"

# Telemetry ClickHouse tables
TEL_STAGING_TABLE="telemetry_events_staging"
TEL_FINAL_TABLE="telemetry_metrics"

# Test Data
TEST_CRM_USER_ID=$((1000 + RANDOM % 9000))
TEST_EMAIL="prothetic3@example.com"

TEST_TEL_ID=$((1000 + RANDOM % 9000))
TEST_METRIC_TYPE="clicks"
TEST_METRIC_VALUE="42.5"

# ----------------------------------------
#            CRM TEST
# ----------------------------------------
echo ""
echo "==============================="
echo "     🔵 CRM PIPELINE TEST"
echo "==============================="

echo "1️⃣ Inserting CRM test row into PostgreSQL..."

docker exec -i $CRM_PG_CONTAINER psql -U $CRM_PG_USER -d $CRM_PG_DB <<SQL
INSERT INTO customers(id, name, email)
VALUES ($TEST_CRM_USER_ID, 'Pipeline Test User', '$TEST_EMAIL');
SQL

echo "   ✅ CRM row inserted"

sleep 5

echo "2️⃣ Checking CRM event in Kafka topic $CRM_KAFKA_TOPIC ..."
CRM_MSG=$(docker exec $KAFKA_CONTAINER kafka-console-consumer \
    --bootstrap-server localhost:9092 \
    --topic $CRM_KAFKA_TOPIC \
    --from-beginning \
    --timeout-ms 10000 \
    --max-messages 1 | tee /dev/tty)

if [[ -z "$CRM_MSG" ]]; then
    echo "   ❌ CRM event did NOT reach Kafka"
    exit 1
fi

echo "   ✅ CRM event found in Kafka"

sleep 5

echo "3️⃣ Checking CRM staging table ($CRM_STAGING_TABLE)..."
CRM_STAGING_ROWS=$(docker exec $CH_CONTAINER clickhouse-client --database $CH_DB --query "
SELECT count(*) FROM $CRM_STAGING_TABLE WHERE id=$TEST_CRM_USER_ID;
")

if [[ "$CRM_STAGING_ROWS" -eq 0 ]]; then
    echo "   ❌ CRM staging table is empty"
    exit 1
fi

echo "   ✅ CRM staging table OK ($CRM_STAGING_ROWS rows)"

sleep 5

echo "4️⃣ Checking CRM final table ($CRM_FINAL_TABLE)..."
CRM_FINAL_ROWS=$(docker exec $CH_CONTAINER clickhouse-client --database $CH_DB --query "
SELECT count(*) FROM $CRM_FINAL_TABLE WHERE id=$TEST_CRM_USER_ID;
")

if [[ "$CRM_FINAL_ROWS" -eq 0 ]]; then
    echo "   ❌ CRM final table did not receive the row"
    exit 1
fi

echo "   🎉 CRM pipeline works correctly!"



# ----------------------------------------
#            TELEMETRY TEST
# ----------------------------------------
echo ""
echo "==============================="
echo "   🟣 TELEMETRY PIPELINE TEST"
echo "==============================="

echo "1️⃣ Inserting Telemetry test event into PostgreSQL..."

docker exec -i $TEL_PG_CONTAINER psql -U $TEL_PG_USER -d $TEL_PG_DB <<SQL
INSERT INTO telemetry(id, client_id, event_time, metric_type, metric_value, payload)
VALUES (
    $TEST_TEL_ID,
    '$TEST_EMAIL',
    NOW(),
    '$TEST_METRIC_TYPE',
    '$TEST_METRIC_VALUE',
    '{"source": "test-script"}'
);
SQL

echo "   ✅ Telemetry event inserted"

sleep 5

echo "2️⃣ Checking telemetry event in Kafka topic $TEL_KAFKA_TOPIC ..."
TEL_MSG=$(docker exec $KAFKA_CONTAINER kafka-console-consumer \
    --bootstrap-server localhost:9092 \
    --topic $TEL_KAFKA_TOPIC \
    --from-beginning \
    --timeout-ms 10000 \
    --max-messages 1 | tee /dev/tty)

if [[ -z "$TEL_MSG" ]]; then
    echo "   ❌ Telemetry event NOT found in Kafka"
    exit 1
fi

echo "   ✅ Telemetry event found in Kafka"

sleep 5

echo "3️⃣ Checking telemetry staging table ($TEL_STAGING_TABLE)..."
TEL_STAGING_ROWS=$(docker exec $CH_CONTAINER clickhouse-client --database $CH_DB --query "
SELECT count(*) FROM $TEL_STAGING_TABLE WHERE id='$TEST_TEL_ID';
")

if [[ "$TEL_STAGING_ROWS" -eq 0 ]]; then
    echo "   ❌ Telemetry staging table did not receive the event"
    exit 1
fi

echo "   ✅ Telemetry staging OK ($TEL_STAGING_ROWS rows)"

sleep 5

echo "4️⃣ Checking final telemetry metrics table ($TEL_FINAL_TABLE)..."
TEL_FINAL_ROWS=$(docker exec $CH_CONTAINER clickhouse-client --database $CH_DB --query "
SELECT count(*) FROM $TEL_FINAL_TABLE WHERE email='$TEST_EMAIL';
")

if [[ "$TEL_FINAL_ROWS" -eq 0 ]]; then
    echo "   ❌ Telemetry aggregated table did NOT receive data"
    exit 1
fi

echo "   🎉 Telemetry pipeline works correctly!"

echo ""
echo "==============================================="
echo "🎯 FULL PIPELINE TEST SUCCESSFUL FOR BOTH:"
echo "   CRM + TELEMETRY → Debezium → Kafka → ClickHouse"
echo "==============================================="
