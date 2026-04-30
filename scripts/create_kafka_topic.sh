#!/usr/bin/env bash
set -euo pipefail

# Create Kafka topic if it does not exist

docker-compose exec kafka kafka-topics --bootstrap-server kafka:9092 \
  --create --topic orders_topic --partitions 1 --replication-factor 1 || true

echo "Topic 'orders_topic' creation attempted."
