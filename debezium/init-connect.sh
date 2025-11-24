#!/bin/sh
echo "⏳ Ждем Connect..."
until curl -s http://connect:8083/connectors >/dev/null 2>&1; do
  sleep 2
done

sleep 5

echo "📄 Регистрируем коннекторы..."

for f in /kafka/connectors/*.properties; do
  filename=$(basename "$f")
  connector_name="${filename%.properties}"

  echo " → Загружаем $f как коннектор: $connector_name"

  CONFIG_JSON=$(awk -F= '
    NF==2 {
      gsub("\"", "\\\"", $2)
      printf "\"%s\":\"%s\",\n", $1, $2
    }
  ' "$f" | sed '$ s/,$//')

  PAYLOAD="{\"name\":\"${connector_name}\",\"config\":{${CONFIG_JSON}}}"

  curl -s -X POST http://connect:8083/connectors \
    -H "Content-Type: application/json" \
    -d "$PAYLOAD"
done


echo "✅ Коннекторы зарегистрированы"
