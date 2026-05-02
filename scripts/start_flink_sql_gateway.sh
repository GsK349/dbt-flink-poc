#!/bin/bash
# Starts a local Flink session cluster (JM + TM) and the SQL Gateway in foreground.
set -euo pipefail

# Configure Flink to bind to all interfaces so JM REST is reachable on localhost:8081
cat > /opt/flink/conf/config.yaml <<EOF
jobmanager:
  rpc:
    address: localhost
    port: 6123
  bind-host: 0.0.0.0
  memory:
    process:
      size: 1600m
taskmanager:
  bind-host: 0.0.0.0
  host: localhost
  numberOfTaskSlots: 4
  memory:
    process:
      size: 2g
parallelism:
  default: 1
rest:
  port: 8081
  address: 0.0.0.0
  bind-address: 0.0.0.0
classloader:
  resolve-order: parent-first
state:
  backend:
    type: hashmap
  checkpoints:
    dir: file:///tmp/flink-checkpoints
  savepoints:
    dir: file:///tmp/flink-savepoints
EOF

mkdir -p /tmp/flink-checkpoints /tmp/flink-savepoints

echo "==> Starting JobManager..."
/opt/flink/bin/jobmanager.sh start

echo "==> Starting TaskManager..."
/opt/flink/bin/taskmanager.sh start

# Wait for JM REST to be available
for i in {1..30}; do
  if curl -sSf http://localhost:8081/overview >/dev/null 2>&1; then
    echo "==> JobManager REST ready"
    break
  fi
  sleep 2
done

echo "==> Starting SQL Gateway in foreground..."
exec /opt/flink/bin/sql-gateway.sh start-foreground \
  -Dsql-gateway.endpoint.rest.address=0.0.0.0 \
  -Dsql-gateway.endpoint.rest.port=8083
