#!/usr/bin/env bash
set -euo pipefail

SQL_FILE=${1:-/opt/flink/sql/flink_kafka_to_iceberg.sql}
LIB_DIR=/opt/flink/usrlib

docker-compose exec jobmanager /opt/flink/bin/sql-client.sh -f "$SQL_FILE"
