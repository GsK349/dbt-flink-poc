#!/usr/bin/env bash
set -euo pipefail

# Publish sample JSON orders to the Kafka topic

cat <<'EOF' | docker-compose exec -T kafka kafka-console-producer \
  --bootstrap-server kafka:9092 --topic orders_topic --property "parse.key=false"
{"id":"1001","customer_id":"cust_1001","order_ts":"2026-04-28 08:00:00","amount":15.50,"status":"created"}
{"id":"1002","customer_id":"cust_1002","order_ts":"2026-04-28 08:05:00","amount":42.00,"status":"completed"}
{"id":"1003","customer_id":"cust_1003","order_ts":"2026-04-28 08:10:00","amount":9.99,"status":"cancelled"}
EOF

echo "Published sample orders to orders_topic."
