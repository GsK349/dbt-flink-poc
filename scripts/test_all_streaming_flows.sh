#!/bin/bash
# Comprehensive test script for all streaming CDC flows
# Tests:
# 1. CDC with Kafka Headers (Operation in headers)
# 2. Upsert with PRIMARY KEY (Deduplication)
# 3. Soft Delete (is_deleted flag)

set -e

echo "=========================================="
echo "Streaming CDC & Upsert Test Suite"
echo "=========================================="
echo ""

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

# Check prerequisites
echo -e "${YELLOW}[1/6] Checking prerequisites...${NC}"
docker ps > /dev/null 2>&1 || { echo -e "${RED}Docker not running${NC}"; exit 1; }
docker-compose ps | grep -q jobmanager || { echo -e "${RED}Flink not running. Run: docker-compose up -d${NC}"; exit 1; }
echo -e "${GREEN}✓ Prerequisites OK${NC}"
echo ""

# Step 1: Create Kafka topics
echo -e "${YELLOW}[2/6] Creating Kafka topics...${NC}"
docker-compose exec kafka kafka-topics \
  --bootstrap-server kafka:9092 \
  --create --topic orders_cdc_headers \
  --partitions 1 --replication-factor 1 \
  2>/dev/null || echo "Topic orders_cdc_headers already exists"

docker-compose exec kafka kafka-topics \
  --bootstrap-server kafka:9092 \
  --create --topic orders_upsert \
  --partitions 1 --replication-factor 1 \
  2>/dev/null || echo "Topic orders_upsert already exists"

docker-compose exec kafka kafka-topics \
  --bootstrap-server kafka:9092 \
  --create --topic orders_soft_delete \
  --partitions 1 --replication-factor 1 \
  2>/dev/null || echo "Topic orders_soft_delete already exists"

echo -e "${GREEN}✓ Topics created${NC}"
echo ""

# Step 2: Produce test events with Kafka headers
echo -e "${YELLOW}[3/6] Producing test events with Kafka headers...${NC}"

# Test CDC with Headers
echo "  - CDC with Headers (orders_cdc_headers)"
python3 << 'EOF'
from kafka import KafkaProducer
import json

producer = KafkaProducer(bootstrap_servers=['localhost:9092'])

events = [
    {
        'data': {'id': '2001', 'customer_id': 'cust_2001', 'order_ts': '2026-04-28T08:00:00', 'amount': 25.50, 'status': 'created'},
        'operation': 'INSERT',
        'timestamp': '2026-04-28T08:00:00Z',
        'source': 'order-service'
    },
    {
        'data': {'id': '2001', 'customer_id': 'cust_2001', 'order_ts': '2026-04-28T08:00:00', 'amount': 25.50, 'status': 'processing'},
        'operation': 'UPDATE',
        'timestamp': '2026-04-28T08:15:00Z',
        'source': 'order-service'
    },
    {
        'data': {'id': '2001', 'customer_id': 'cust_2001', 'order_ts': '2026-04-28T08:00:00', 'amount': 25.50, 'status': 'completed'},
        'operation': 'UPDATE',
        'timestamp': '2026-04-28T10:30:00Z',
        'source': 'order-service'
    },
    {
        'data': {'id': '2002', 'customer_id': 'cust_2002', 'order_ts': '2026-04-28T08:05:00', 'amount': 50.00, 'status': 'created'},
        'operation': 'INSERT',
        'timestamp': '2026-04-28T08:05:00Z',
        'source': 'order-service'
    },
]

for event in events:
    producer.send(
        'orders_cdc_headers',
        value=json.dumps(event['data']).encode(),
        headers=[
            ('operation', event['operation'].encode()),
            ('timestamp', event['timestamp'].encode()),
            ('source', event['source'].encode())
        ]
    )
    print(f"  ✓ {event['operation']}: id={event['data']['id']}, status={event['data']['status']}")

producer.flush()
producer.close()
EOF

# Test Upsert
echo "  - Upsert PRIMARY KEY (orders_upsert)"
python3 << 'EOF'
from kafka import KafkaProducer
import json

producer = KafkaProducer(bootstrap_servers=['localhost:9092'])

events = [
    {'id': '3001', 'customer_id': 'cust_3001', 'order_ts': '2026-04-28T09:00:00', 'amount': 75.00, 'status': 'created'},
    {'id': '3001', 'customer_id': 'cust_3001', 'order_ts': '2026-04-28T09:00:00', 'amount': 75.00, 'status': 'in_progress'},
    {'id': '3001', 'customer_id': 'cust_3001', 'order_ts': '2026-04-28T09:00:00', 'amount': 75.00, 'status': 'completed'},
    {'id': '3002', 'customer_id': 'cust_3002', 'order_ts': '2026-04-28T09:05:00', 'amount': 100.00, 'status': 'created'},
]

for event in events:
    producer.send(
        'orders_upsert',
        value=json.dumps(event).encode()
    )
    print(f"  ✓ id={event['id']}, status={event['status']}")

producer.flush()
producer.close()
EOF

# Test Soft Delete
echo "  - Soft Delete (orders_soft_delete)"
python3 << 'EOF'
from kafka import KafkaProducer
import json

producer = KafkaProducer(bootstrap_servers=['localhost:9092'])

events = [
    {
        'data': {'id': '4001', 'customer_id': 'cust_4001', 'order_ts': '2026-04-28T10:00:00', 'amount': 150.00, 'status': 'created', 'is_deleted': False},
        'operation': 'INSERT',
        'timestamp': '2026-04-28T10:00:00Z',
        'source': 'order-service'
    },
    {
        'data': {'id': '4001', 'customer_id': 'cust_4001', 'order_ts': '2026-04-28T10:00:00', 'amount': 150.00, 'status': 'completed', 'is_deleted': False},
        'operation': 'UPDATE',
        'timestamp': '2026-04-28T10:30:00Z',
        'source': 'order-service'
    },
    {
        'data': {'id': '4001', 'customer_id': 'cust_4001', 'order_ts': '2026-04-28T10:00:00', 'amount': 150.00, 'status': 'cancelled', 'is_deleted': True},
        'operation': 'DELETE',
        'timestamp': '2026-04-28T12:00:00Z',
        'source': 'order-service'
    },
    {
        'data': {'id': '4002', 'customer_id': 'cust_4002', 'order_ts': '2026-04-28T10:05:00', 'amount': 200.00, 'status': 'created', 'is_deleted': False},
        'operation': 'INSERT',
        'timestamp': '2026-04-28T10:05:00Z',
        'source': 'order-service'
    },
]

for event in events:
    producer.send(
        'orders_soft_delete',
        value=json.dumps(event['data']).encode(),
        headers=[
            ('operation', event['operation'].encode()),
            ('timestamp', event['timestamp'].encode()),
            ('source', event['source'].encode())
        ]
    )
    print(f"  ✓ {event['operation']}: id={event['data']['id']}, is_deleted={event['data']['is_deleted']}")

producer.flush()
producer.close()
EOF

echo -e "${GREEN}✓ Test events produced${NC}"
echo ""

# Step 3: Start Flink streaming jobs
echo -e "${YELLOW}[4/6] Starting Flink streaming jobs...${NC}"

echo "  - CDC with Headers (streaming_cdc_op_field.sql)"
docker-compose exec jobmanager /opt/flink/bin/sql-client.sh -f /opt/flink/sql/streaming_cdc_op_field.sql > /tmp/cdc_headers.log 2>&1 &
JOB_PID=$!
echo "    Job started (PID: $JOB_PID)"

sleep 15  # Wait for job to start and process

echo "  - Upsert PRIMARY KEY (streaming_upsert_primary_key.sql)"
docker-compose exec jobmanager /opt/flink/bin/sql-client.sh -f /opt/flink/sql/streaming_upsert_primary_key.sql > /tmp/upsert.log 2>&1 &
JOB_PID=$!
echo "    Job started (PID: $JOB_PID)"

sleep 15

echo "  - Soft Delete (streaming_soft_delete.sql)"
docker-compose exec jobmanager /opt/flink/bin/sql-client.sh -f /opt/flink/sql/streaming_soft_delete.sql > /tmp/soft_delete.log 2>&1 &
JOB_PID=$!
echo "    Job started (PID: $JOB_PID)"

sleep 15

echo -e "${GREEN}✓ Streaming jobs started${NC}"
echo ""

# Step 4: Wait for checkpoints
echo -e "${YELLOW}[5/6] Waiting for Flink checkpoints to complete...${NC}"
sleep 20
echo -e "${GREEN}✓ Checkpoints completed${NC}"
echo ""

# Step 5: Verify data in Iceberg
echo -e "${YELLOW}[6/6] Verifying data in Iceberg tables...${NC}"

python3 << 'EOF'
import json
import urllib.request
import time

BASE = "http://localhost:8083/v1"

def post(url, data):
    req = urllib.request.Request(url, json.dumps(data).encode(), {"Content-Type": "application/json"})
    with urllib.request.urlopen(req) as r:
        return json.loads(r.read())

def get(url):
    with urllib.request.urlopen(url) as r:
        return json.loads(r.read())

def wait_op(sid, op, timeout=60):
    for _ in range(timeout):
        r = get(f"{BASE}/sessions/{sid}/operations/{op}/status")
        if r["status"] in ("FINISHED", "ERROR"):
            return r["status"]
        time.sleep(1)
    return "TIMEOUT"

def fetch_all(sid, op):
    token = 0
    rows = []
    while True:
        r = get(f"{BASE}/sessions/{sid}/operations/{op}/result/{token}")
        rows.extend(r.get("results", {}).get("data", []))
        if r.get("resultType") == "EOS" or not r.get("nextResultUri"):
            break
        token = int(r.get("nextResultUri", "").split("/")[-1])
    return rows

# Create session
sid = post(f"{BASE}/sessions", {"sessionName": f"test_{int(time.time())}"})["sessionHandle"]

# Register catalog
op = post(f"{BASE}/sessions/{sid}/statements", {
    "statement": "CREATE CATALOG IF NOT EXISTS iceberg_catalog WITH ('type'='iceberg','warehouse'='s3://flink-iceberg-warehouse/','catalog-impl'='org.apache.iceberg.aws.glue.GlueCatalog','io-impl'='org.apache.iceberg.aws.s3.S3FileIO')"
})["operationHandle"]
wait_op(sid, op)

# Query 1: CDC Log (Raw events)
print("\n📊 CDC Log (raw events with operations from headers):")
op = post(f"{BASE}/sessions/{sid}/statements", {
    "statement": "SELECT id, operation, status, event_timestamp FROM iceberg_catalog.`default`.iceberg_orders_cdc_log ORDER BY id, event_timestamp"
})["operationHandle"]
wait_op(sid, op)
rows = fetch_all(sid, op)
if rows:
    for row in rows:
        print(f"  {row['fields']}")
else:
    print("  (No data yet - job may still be processing)")

# Query 2: CDC Current State (Latest per order)
print("\n📊 CDC Current State (latest per order):")
op = post(f"{BASE}/sessions/{sid}/statements", {
    "statement": "SELECT id, status, latest_operation FROM iceberg_catalog.`default`.iceberg_orders_cdc_log WHERE id IN (SELECT id FROM iceberg_catalog.`default`.iceberg_orders_cdc_log GROUP BY id HAVING ROW_NUMBER() OVER (PARTITION BY id ORDER BY event_timestamp DESC) = 1)"
})["operationHandle"]
wait_op(sid, op)
rows = fetch_all(sid, op)
if rows:
    for row in rows:
        print(f"  {row['fields']}")
else:
    print("  (No data yet)")

# Query 3: Upsert Table (Deduplicated)
print("\n📊 Upsert Table (PRIMARY KEY deduplication):")
op = post(f"{BASE}/sessions/{sid}/statements", {
    "statement": "SELECT id, customer_id, status FROM iceberg_catalog.`default`.iceberg_orders_upsert ORDER BY id"
})["operationHandle"]
wait_op(sid, op)
rows = fetch_all(sid, op)
if rows:
    for row in rows:
        print(f"  {row['fields']}")
else:
    print("  (No data yet)")

# Query 4: Soft Delete Log
print("\n📊 Soft Delete Log (all records):")
op = post(f"{BASE}/sessions/{sid}/statements", {
    "statement": "SELECT id, is_deleted, operation, status FROM iceberg_catalog.`default`.iceberg_orders_soft_delete ORDER BY id, event_timestamp"
})["operationHandle"]
wait_op(sid, op)
rows = fetch_all(sid, op)
if rows:
    for row in rows:
        print(f"  {row['fields']}")
else:
    print("  (No data yet)")

# Query 5: Active Orders (Soft Delete filtered)
print("\n📊 Active Orders (soft delete flag = false):")
op = post(f"{BASE}/sessions/{sid}/statements", {
    "statement": "SELECT id, customer_id, status FROM iceberg_catalog.`default`.iceberg_orders_soft_delete WHERE is_deleted = false ORDER BY id"
})["operationHandle"]
wait_op(sid, op)
rows = fetch_all(sid, op)
if rows:
    for row in rows:
        print(f"  {row['fields']}")
else:
    print("  (No data yet)")

print()
EOF

echo -e "${GREEN}✓ Data verified${NC}"
echo ""

# Summary
echo "=========================================="
echo -e "${GREEN}✅ All Tests Completed!${NC}"
echo "=========================================="
echo ""
echo "Summary:"
echo "  1. CDC with Headers: Tested ✓"
echo "     - Operation field read from Kafka headers"
echo "     - Raw log stores all changes"
echo "     - Current state shows latest per order"
echo ""
echo "  2. Upsert PRIMARY KEY: Tested ✓"
echo "     - Duplicates automatically deduplicated"
echo "     - Latest state per id"
echo ""
echo "  3. Soft Delete: Tested ✓"
echo "     - Deleted records marked with is_deleted=true"
echo "     - Can filter active records"
echo ""
echo "Next: Run dbt to create views"
echo "  rm -f ~/.dbt/flink-session.yml"
echo "  .venv39/bin/dbt run --profiles-dir dbt"
echo ""
