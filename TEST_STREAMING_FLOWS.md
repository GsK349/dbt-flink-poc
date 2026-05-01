# Testing All Streaming CDC Flows — Step-by-Step Guide

This guide walks through testing all three streaming CDC patterns:
1. **CDC with Kafka Headers** (operation in headers, not payload)
2. **Upsert with PRIMARY KEY** (deduplication)
3. **Soft Delete** (is_deleted flag + headers)

---

## Quick Start (Automated)

Run the test script:
```bash
bash scripts/test_all_streaming_flows.sh
```

This will:
- Create Kafka topics
- Produce test events with headers
- Start Flink streaming jobs
- Wait for checkpoints
- Query and display results

---

## Manual Testing (Step-by-Step)

### Prerequisites

```bash
# 1. Start Docker stack
docker-compose up -d

# 2. Wait for all containers to be healthy
docker ps | grep flink_dbt_poc

# 3. Verify Flink is ready
curl http://localhost:8081/overview
```

---

## Test 1: CDC with Kafka Headers

### Step 1.1: Create Topic

```bash
docker-compose exec kafka kafka-topics \
  --bootstrap-server kafka:9092 \
  --create --topic orders_cdc_headers \
  --partitions 1 --replication-factor 1
```

### Step 1.2: Produce Events with Headers

```bash
python3 << 'EOF'
from kafka import KafkaProducer
import json

producer = KafkaProducer(bootstrap_servers=['localhost:9092'])

# Event 1: INSERT order 2001
producer.send(
    'orders_cdc_headers',
    value=json.dumps({
        "id": "2001",
        "customer_id": "cust_2001",
        "order_ts": "2026-04-28T08:00:00",
        "amount": 25.50,
        "status": "created"
    }).encode(),
    headers=[
        ('operation', b'INSERT'),
        ('timestamp', b'2026-04-28T08:00:00Z'),
        ('source', b'order-service')
    ]
)
print("✓ INSERT: order 2001 created")

# Event 2: UPDATE order 2001 status
producer.send(
    'orders_cdc_headers',
    value=json.dumps({
        "id": "2001",
        "customer_id": "cust_2001",
        "order_ts": "2026-04-28T08:00:00",
        "amount": 25.50,
        "status": "processing"
    }).encode(),
    headers=[
        ('operation', b'UPDATE'),
        ('timestamp', b'2026-04-28T08:15:00Z'),
        ('source', b'order-service')
    ]
)
print("✓ UPDATE: order 2001 → processing")

# Event 3: UPDATE order 2001 to completed
producer.send(
    'orders_cdc_headers',
    value=json.dumps({
        "id": "2001",
        "customer_id": "cust_2001",
        "order_ts": "2026-04-28T08:00:00",
        "amount": 25.50,
        "status": "completed"
    }).encode(),
    headers=[
        ('operation', b'UPDATE'),
        ('timestamp', b'2026-04-28T10:30:00Z'),
        ('source', b'order-service')
    ]
)
print("✓ UPDATE: order 2001 → completed")

# Event 4: INSERT order 2002
producer.send(
    'orders_cdc_headers',
    value=json.dumps({
        "id": "2002",
        "customer_id": "cust_2002",
        "order_ts": "2026-04-28T08:05:00",
        "amount": 50.00,
        "status": "created"
    }).encode(),
    headers=[
        ('operation', b'INSERT'),
        ('timestamp', b'2026-04-28T08:05:00Z'),
        ('source', b'order-service')
    ]
)
print("✓ INSERT: order 2002 created")

producer.flush()
producer.close()
EOF
```

### Step 1.3: Start Flink Streaming Job

```bash
docker-compose exec jobmanager \
  /opt/flink/bin/sql-client.sh -f /opt/flink/sql/streaming_cdc_op_field.sql
```

### Step 1.4: Wait for Checkpoint

Wait ~20 seconds for Flink to process and checkpoint.

### Step 1.5: Query Results

```bash
python3 << 'EOF'
import json, urllib.request, time

BASE = "http://localhost:8083/v1"

def post(url, data):
    req = urllib.request.Request(url, json.dumps(data).encode(), {"Content-Type": "application/json"})
    with urllib.request.urlopen(req) as r:
        return json.loads(r.read())

def get(url):
    with urllib.request.urlopen(url) as r:
        return json.loads(r.read())

def wait_op(sid, op):
    for _ in range(60):
        r = get(f"{BASE}/sessions/{sid}/operations/{op}/status")
        if r["status"] in ("FINISHED", "ERROR"):
            return r["status"]
        time.sleep(1)

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
sid = post(f"{BASE}/sessions", {"sessionName": "test_cdc"})["sessionHandle"]

# Register catalog
op = post(f"{BASE}/sessions/{sid}/statements", {
    "statement": "CREATE CATALOG IF NOT EXISTS iceberg_catalog WITH ('type'='iceberg','warehouse'='s3://flink-iceberg-warehouse/','catalog-impl'='org.apache.iceberg.aws.glue.GlueCatalog','io-impl'='org.apache.iceberg.aws.s3.S3FileIO')"
})["operationHandle"]
wait_op(sid, op)

# Query: Raw CDC log
print("CDC Log (all events):")
op = post(f"{BASE}/sessions/{sid}/statements", {
    "statement": "SELECT id, operation, status, event_timestamp FROM iceberg_catalog.`default`.iceberg_orders_cdc_log ORDER BY id, event_timestamp"
})["operationHandle"]
wait_op(sid, op)
rows = fetch_all(sid, op)
for row in rows:
    print(f"  {row['fields']}")
EOF
```

**Expected Output:**
```
CDC Log (all events):
  ['2001', 'INSERT', 'created', '2026-04-28T08:00:00']
  ['2001', 'UPDATE', 'processing', '2026-04-28T08:15:00']
  ['2001', 'UPDATE', 'completed', '2026-04-28T10:30:00']
  ['2002', 'INSERT', 'created', '2026-04-28T08:05:00']
```

---

## Test 2: Upsert with PRIMARY KEY

### Step 2.1: Create Topic

```bash
docker-compose exec kafka kafka-topics \
  --bootstrap-server kafka:9092 \
  --create --topic orders_upsert \
  --partitions 1 --replication-factor 1
```

### Step 2.2: Produce Events (No Headers Needed)

```bash
python3 << 'EOF'
from kafka import KafkaProducer
import json

producer = KafkaProducer(bootstrap_servers=['localhost:9092'])

# Same order 3001 with different statuses
events = [
    {"id": "3001", "customer_id": "cust_3001", "order_ts": "2026-04-28T09:00:00", "amount": 75.00, "status": "created"},
    {"id": "3001", "customer_id": "cust_3001", "order_ts": "2026-04-28T09:00:00", "amount": 75.00, "status": "in_progress"},
    {"id": "3001", "customer_id": "cust_3001", "order_ts": "2026-04-28T09:00:00", "amount": 75.00, "status": "completed"},
    {"id": "3002", "customer_id": "cust_3002", "order_ts": "2026-04-28T09:05:00", "amount": 100.00, "status": "created"},
]

for event in events:
    producer.send('orders_upsert', value=json.dumps(event).encode())
    print(f"✓ {event['id']}: {event['status']}")

producer.flush()
producer.close()
EOF
```

### Step 2.3: Start Flink Job

```bash
docker-compose exec jobmanager \
  /opt/flink/bin/sql-client.sh -f /opt/flink/sql/streaming_upsert_primary_key.sql
```

### Step 2.4: Wait & Query

Wait 20 seconds, then:

```sql
SELECT id, status FROM iceberg_catalog.`default`.iceberg_orders_upsert ORDER BY id;
```

**Expected Output:**
```
id    | status
------|----------
3001  | completed   ← Latest (not created or in_progress)
3002  | created
```

Only **2 rows** (deduplicated by PRIMARY KEY).

---

## Test 3: Soft Delete

### Step 3.1: Create Topic

```bash
docker-compose exec kafka kafka-topics \
  --bootstrap-server kafka:9092 \
  --create --topic orders_soft_delete \
  --partitions 1 --replication-factor 1
```

### Step 3.2: Produce Events with Headers & is_deleted Flag

```bash
python3 << 'EOF'
from kafka import KafkaProducer
import json

producer = KafkaProducer(bootstrap_servers=['localhost:9092'])

# Event 1: INSERT
producer.send(
    'orders_soft_delete',
    value=json.dumps({
        "id": "4001",
        "customer_id": "cust_4001",
        "order_ts": "2026-04-28T10:00:00",
        "amount": 150.00,
        "status": "created",
        "is_deleted": False
    }).encode(),
    headers=[('operation', b'INSERT'), ('timestamp', b'2026-04-28T10:00:00Z')]
)
print("✓ INSERT: order 4001")

# Event 2: UPDATE
producer.send(
    'orders_soft_delete',
    value=json.dumps({
        "id": "4001",
        "customer_id": "cust_4001",
        "order_ts": "2026-04-28T10:00:00",
        "amount": 150.00,
        "status": "completed",
        "is_deleted": False
    }).encode(),
    headers=[('operation', b'UPDATE'), ('timestamp', b'2026-04-28T10:30:00Z')]
)
print("✓ UPDATE: order 4001")

# Event 3: DELETE (soft delete - mark as deleted)
producer.send(
    'orders_soft_delete',
    value=json.dumps({
        "id": "4001",
        "customer_id": "cust_4001",
        "order_ts": "2026-04-28T10:00:00",
        "amount": 150.00,
        "status": "cancelled",
        "is_deleted": True
    }).encode(),
    headers=[('operation', b'DELETE'), ('timestamp', b'2026-04-28T12:00:00Z')]
)
print("✓ DELETE: order 4001 (soft delete)")

# Event 4: Active order
producer.send(
    'orders_soft_delete',
    value=json.dumps({
        "id": "4002",
        "customer_id": "cust_4002",
        "order_ts": "2026-04-28T10:05:00",
        "amount": 200.00,
        "status": "created",
        "is_deleted": False
    }).encode(),
    headers=[('operation', b'INSERT'), ('timestamp', b'2026-04-28T10:05:00Z')]
)
print("✓ INSERT: order 4002")

producer.flush()
producer.close()
EOF
```

### Step 3.3: Start Flink Job

```bash
docker-compose exec jobmanager \
  /opt/flink/bin/sql-client.sh -f /opt/flink/sql/streaming_soft_delete.sql
```

### Step 3.4: Query Results

**All records (with soft delete flag):**
```sql
SELECT id, is_deleted, operation FROM iceberg_catalog.`default`.iceberg_orders_soft_delete ORDER BY id;
```

**Expected:**
```
id    | is_deleted | operation
------|------------|----------
4001  | true       | DELETE       ← Marked as deleted
4002  | false      | INSERT
```

**Active records only (filter is_deleted = false):**
```sql
SELECT id, status FROM iceberg_catalog.`default`.iceberg_orders_soft_delete WHERE is_deleted = false;
```

**Expected:**
```
id    | status
------|--------
4002  | created
```

Only **1 row** (order 4001 is hidden).

---

## Test with dbt

After data is in Iceberg, materialize views with dbt:

```bash
# Clear stale session
rm -f ~/.dbt/flink-session.yml

# Run dbt
.venv39/bin/dbt run --profiles-dir dbt
```

### Test dbt Views

```bash
# Query dbt materialized view
SELECT * FROM mart_orders_cdc_current;
SELECT * FROM mart_orders_active;
```

---

## Verification Checklist

- [ ] CDC with Headers: All 4 events stored, operations from headers
- [ ] Upsert: Only 2 rows (deduplicated by id)
- [ ] Soft Delete: Deleted order marked with is_deleted=true
- [ ] dbt runs without errors
- [ ] Views query data correctly

---

## Troubleshooting

### No data appearing in Iceberg

Check Flink job status:
```bash
curl http://localhost:8081/jobs
```

If job is RUNNING but no data:
- Wait another 20 seconds (checkpoint interval)
- Check job logs: `docker-compose logs jobmanager`

### Headers not being read

Verify Kafka events have headers:
```bash
python3 << 'EOF'
from kafka import KafkaConsumer

consumer = KafkaConsumer('orders_cdc_headers', bootstrap_servers=['localhost:9092'])
for msg in consumer:
    print(f"Headers: {msg.headers}")
    print(f"Value: {msg.value}")
    break
EOF
```

### Flink SQL error

Check if catalog registration works:
```bash
# Test SQL Gateway
python3 << 'EOF'
import json, urllib.request

url = "http://localhost:8083/v1/sessions"
req = urllib.request.Request(url, json.dumps({"sessionName": "test"}).encode(), {"Content-Type": "application/json"})
with urllib.request.urlopen(req) as r:
    print(json.loads(r.read()))
EOF
```

---

## Next Steps

1. ✅ Test all flows (this guide)
2. ✅ Verify data in Iceberg
3. ✅ Run dbt to create views
4. → Deploy to production with similar patterns
