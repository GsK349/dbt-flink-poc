# Streaming CDC (Change Data Capture) & Upserts — Complete Guide

This guide explains how to handle INSERT/UPDATE/DELETE operations in a streaming pipeline where events arrive from Kafka.

---

## Table of Contents

1. [The Problem](#1-the-problem)
2. [Pattern 1: Operation Field (Recommended)](#pattern-1-operation-field-recommended)
3. [Pattern 2: Upsert Semantics (Duplicates)](#pattern-2-upsert-semantics-duplicates)
4. [Pattern 3: Soft Deletes](#pattern-3-soft-deletes)
5. [Pattern 4: Debezium CDC Format](#pattern-4-debezium-cdc-format)
6. [Pattern 5: Event Versioning](#pattern-5-event-versioning)
7. [Choosing the Right Pattern](#7-choosing-the-right-pattern)

---

## 1. The Problem

### Current Setup
```
Kafka: orders_topic
  {"id":"1001", "customer_id":"cust_1001", "amount":15.50, "status":"created"}
  {"id":"1002", "customer_id":"cust_1002", "amount":42.00, "status":"completed"}
  
Flink: Always INSERTs (appends) rows
  ↓
Iceberg:
  Row 1: id=1001, status='created'
  Row 2: id=1001, status='created'  ← Duplicate!
  Row 3: id=1001, status='created'  ← Duplicate!
  Row 4: id=1002, status='completed'
```

**Problem**: Order 1001 appears 3 times. If the status should have changed to 'completed', you don't know which version is correct.

### What We Need

```
Kafka: orders_topic
  {"id":"1001", "op":"INSERT", "status":"created"}
  {"id":"1001", "op":"UPDATE", "status":"in_progress"}
  {"id":"1001", "op":"UPDATE", "status":"completed"}
  {"id":"1002", "op":"DELETE"}
  
Flink: Respects the operation field
  ↓
Iceberg:
  Row 1: id=1001, status='created' → updated
  Row 2: id=1001, status='in_progress' → updated
  Row 3: id=1001, status='completed' ← Final state
  Row 4: id=1002 → deleted
```

---

## 2. Pattern 1: Operation Field (Recommended)

### Concept
Each Kafka event includes an `op` field indicating the operation:
- `op: 'INSERT'` → Add new row
- `op: 'UPDATE'` → Modify existing row
- `op: 'DELETE'` → Remove row

### Step 1: Kafka Event Format

```json
{
  "id": "1001",
  "customer_id": "cust_1001",
  "order_ts": "2026-04-28T08:00:00",
  "amount": 15.50,
  "status": "created",
  "op": "INSERT",
  "ts": "2026-04-28T08:00:00"
}

{
  "id": "1001",
  "customer_id": "cust_1001",
  "order_ts": "2026-04-28T08:00:00",
  "amount": 15.50,
  "status": "completed",
  "op": "UPDATE",
  "ts": "2026-04-28T10:30:00"
}

{
  "id": "1001",
  "customer_id": "cust_1001",
  "order_ts": "2026-04-28T08:00:00",
  "amount": 15.50,
  "status": "cancelled",
  "op": "DELETE",
  "ts": "2026-04-28T12:00:00"
}
```

### Step 2: Create Kafka Source with Operation Field

Create `sql/streaming_cdc_with_op_field.sql`:

```sql
SET 'execution.runtime-mode' = 'streaming';
SET 'execution.checkpointing.interval' = '10s';
SET 'execution.checkpointing.mode' = 'EXACTLY_ONCE';

USE CATALOG default_catalog;
USE default_database;

-- Kafka source with operation field
CREATE TABLE IF NOT EXISTS orders_cdc (
  id STRING,
  customer_id STRING,
  order_ts TIMESTAMP(3),
  amount DECIMAL(10,2),
  status STRING,
  op STRING,                    -- INSERT, UPDATE, DELETE
  ts TIMESTAMP(3),              -- event timestamp
  WATERMARK FOR order_ts AS order_ts - INTERVAL '0' SECOND
) WITH (
  'connector' = 'kafka',
  'topic' = 'orders_cdc_topic',
  'properties.bootstrap.servers' = 'kafka:9092',
  'scan.startup.mode' = 'earliest-offset',
  'format' = 'json',
  'json.timestamp-format.standard' = 'ISO-8601'
);

-- Set up Iceberg catalog
CREATE CATALOG IF NOT EXISTS iceberg_catalog WITH (
  'type' = 'iceberg',
  'warehouse' = 's3://flink-iceberg-warehouse/',
  'catalog-impl' = 'org.apache.iceberg.aws.glue.GlueCatalog',
  'io-impl' = 'org.apache.iceberg.aws.s3.S3FileIO'
);

-- Iceberg table
CREATE TABLE IF NOT EXISTS iceberg_catalog.`default`.iceberg_orders_upsert (
  id STRING,
  customer_id STRING,
  order_ts TIMESTAMP(3),
  amount DECIMAL(10,2),
  status STRING
) WITH (
  'write.format.default' = 'parquet'
);

-- Stream: Apply CDC operations
-- For each event, check operation type
INSERT INTO iceberg_catalog.`default`.iceberg_orders_upsert
SELECT id, customer_id, order_ts, amount, status
FROM orders_cdc
WHERE op = 'INSERT';

-- For UPDATE and DELETE, we need to use a different approach
-- Flink INSERT doesn't support UPDATE/DELETE directly
-- Solution: Use a temp table + dbt for transformations (see below)
```

### Step 3: Handle UPDATE/DELETE with State

Since Flink INSERT only appends, we need to **manage state differently**. Use this approach:

```sql
-- Instead of directly manipulating Iceberg, use a temp table in streaming
-- and apply updates/deletes in a separate batch job

-- Create a raw events table (append-only log)
CREATE TABLE IF NOT EXISTS iceberg_catalog.`default`.iceberg_orders_raw_cdc (
  id STRING,
  customer_id STRING,
  order_ts TIMESTAMP(3),
  amount DECIMAL(10,2),
  status STRING,
  op STRING,
  event_ts TIMESTAMP(3)
);

-- Stream all events (INSERT, UPDATE, DELETE) as raw logs
INSERT INTO iceberg_catalog.`default`.iceberg_orders_raw_cdc
SELECT id, customer_id, order_ts, amount, status, op, ts
FROM orders_cdc;

-- Then in dbt (or batch job), materialize the final state:
-- SELECT id, customer_id, order_ts, amount, status
-- FROM (
--   SELECT *,
--          ROW_NUMBER() OVER (PARTITION BY id ORDER BY event_ts DESC) as rn
--   FROM iceberg_orders_raw_cdc
--   WHERE op != 'DELETE'
-- ) WHERE rn = 1;  -- Get latest non-deleted version
```

### Step 4: dbt Model to Materialize Final State

Create `dbt/models/stg_orders_cdc.sql`:

```sql
{{ config(materialized='view') }}

SELECT
  id,
  customer_id,
  order_ts,
  amount,
  status
FROM (
  SELECT
    id,
    customer_id,
    order_ts,
    amount,
    status,
    op,
    event_ts,
    ROW_NUMBER() OVER (PARTITION BY id ORDER BY event_ts DESC) as rn
  FROM iceberg_catalog.`default`.iceberg_orders_raw_cdc
)
WHERE op != 'DELETE'  -- Exclude deleted records
  AND rn = 1          -- Keep only latest version per order
```

### Advantages
✅ Simple to understand (explicit operations)
✅ Transparent audit trail (can see all changes)
✅ Works with any source system

### Disadvantages
❌ Requires coordination: source must send `op` field
❌ Extra storage (raw CDC log)
❌ Batch job needed for final materialization

---

## 3. Pattern 2: Upsert Semantics (Duplicates)

### Concept
Instead of tracking operations, rely on **primary keys**. Latest event for a key wins.

```
Order 1001 appears 3 times with different data:
  Event 1: id=1001, status='created', ts=08:00
  Event 2: id=1001, status='in_progress', ts=08:15
  Event 3: id=1001, status='completed', ts=10:30
  
Upsert rule: Keep the one with latest timestamp
Result: id=1001, status='completed'
```

### Step 1: Define Primary Key

```sql
SET 'execution.runtime-mode' = 'streaming';

CREATE TABLE IF NOT EXISTS orders_stream (
  id STRING,
  customer_id STRING,
  order_ts TIMESTAMP(3),
  amount DECIMAL(10,2),
  status STRING,
  ts TIMESTAMP(3),
  PRIMARY KEY (id) NOT ENFORCED  -- Tell Flink: 'id' is unique
) WITH (
  'connector' = 'kafka',
  'topic' = 'orders_topic',
  'properties.bootstrap.servers' = 'kafka:9092',
  'scan.startup.mode' = 'earliest-offset',
  'format' = 'json',
  'json.timestamp-format.standard' = 'ISO-8601'
);

-- Iceberg sink with upsert support
-- Flink will deduplicate based on primary key
INSERT INTO iceberg_catalog.`default`.iceberg_orders
SELECT id, customer_id, order_ts, amount, status
FROM orders_stream;
```

### Step 2: Flink Handles Deduplication

When the same `id` appears multiple times, Flink's Iceberg connector:
1. Looks up the key in Iceberg
2. If found → UPDATE
3. If not found → INSERT

### Result
```
Iceberg (final state after streaming):
  id=1001, status='completed' (latest)
  id=1002, status='completed'
```

### Advantages
✅ Simple (no `op` field needed)
✅ Automatic deduplication
✅ No batch job required

### Disadvantages
❌ No audit trail (can't see old values)
❌ Requires well-defined primary keys
❌ Higher write cost (many updates)

### Testing Upsert Semantics

```bash
# Publish same order with different statuses
cat <<'EOF' | docker-compose exec -T kafka kafka-console-producer \
  --bootstrap-server kafka:9092 --topic orders_topic
{"id":"1001","customer_id":"cust_1001","order_ts":"2026-04-28T08:00:00","amount":15.50,"status":"created"}
{"id":"1001","customer_id":"cust_1001","order_ts":"2026-04-28T08:00:00","amount":15.50,"status":"in_progress"}
{"id":"1001","customer_id":"cust_1001","order_ts":"2026-04-28T08:00:00","amount":15.50,"status":"completed"}
EOF

# Query Iceberg - should see only the latest status
SELECT * FROM iceberg_orders WHERE id = '1001';
# Result: one row with status='completed'
```

---

## 4. Pattern 3: Soft Deletes

### Concept
Instead of actually deleting, add an `is_deleted` flag.

```
Event: {"id":"1001", "is_deleted": true}

Iceberg:
  id=1001, status='completed', is_deleted=false
  → Update: is_deleted=true

Query: SELECT * FROM orders WHERE is_deleted = false
Result: Order 1001 is hidden (but data preserved)
```

### Implementation

```sql
SET 'execution.runtime-mode' = 'streaming';

CREATE TABLE IF NOT EXISTS orders_with_delete_flag (
  id STRING,
  customer_id STRING,
  order_ts TIMESTAMP(3),
  amount DECIMAL(10,2),
  status STRING,
  is_deleted BOOLEAN,  -- Add this field
  PRIMARY KEY (id) NOT ENFORCED
) WITH (
  'connector' = 'kafka',
  'topic' = 'orders_topic',
  'properties.bootstrap.servers' = 'kafka:9092',
  'scan.startup.mode' = 'earliest-offset',
  'format' = 'json',
  'json.timestamp-format.standard' = 'ISO-8601'
);

INSERT INTO iceberg_catalog.`default`.iceberg_orders
SELECT id, customer_id, order_ts, amount, status, is_deleted
FROM orders_with_delete_flag;
```

### Query with Soft Deletes

```sql
-- Always filter out deleted records
SELECT * FROM iceberg_orders WHERE is_deleted = FALSE;

-- But audit trail preserves deletion records
SELECT * FROM iceberg_orders WHERE id = '1001';
-- Returns: id=1001, is_deleted=true (shows it was deleted)
```

### Advantages
✅ Preserves audit trail (can see deletions)
✅ Can undelete easily
✅ Works with upsert deduplication

### Disadvantages
❌ Extra column in every row
❌ Queries must always filter `is_deleted = FALSE`
❌ Doesn't actually free storage space

---

## 5. Pattern 4: Debezium CDC Format

### Concept
Use **Debezium** (open-source CDC tool) to capture changes from a database and stream them to Kafka in standard format.

### Debezium Event Format

```json
{
  "before": {
    "id": 1001,
    "status": "created"
  },
  "after": {
    "id": 1001,
    "status": "completed"
  },
  "op": "u",  -- "c" = create, "u" = update, "d" = delete
  "ts_ms": 1619529600000
}
```

### Flink SQL with Debezium

```sql
SET 'execution.runtime-mode' = 'streaming';

CREATE TABLE IF NOT EXISTS orders_debezium (
  id STRING,
  customer_id STRING,
  order_ts TIMESTAMP(3),
  amount DECIMAL(10,2),
  status STRING
) WITH (
  'connector' = 'kafka',
  'topic' = 'debezium-orders',
  'properties.bootstrap.servers' = 'kafka:9092',
  'format' = 'debezium-json',  -- Special Debezium format
  'debezium-json.schema.include' = 'false'
);

INSERT INTO iceberg_catalog.`default`.iceberg_orders
SELECT id, customer_id, order_ts, amount, status
FROM orders_debezium;
```

### Advantages
✅ Industry standard (works with databases)
✅ Rich metadata (before/after values)
✅ Automatic CDC from PostgreSQL, MySQL, Oracle, etc.

### Disadvantages
❌ Requires Debezium setup
❌ More complex infrastructure
❌ Overkill if you control the source

---

## 6. Pattern 5: Event Versioning

### Concept
Each entity has a version. Latest version wins.

```
Order 1001 events:
  Event 1: {"id":"1001", "version":1, "status":"created"}
  Event 2: {"id":"1001", "version":2, "status":"in_progress"}
  Event 3: {"id":"1001", "version":3, "status":"completed"}

Keep only: version=3 (latest)
```

### Implementation

```sql
SET 'execution.runtime-mode' = 'batch';

-- Denormalized latest state
CREATE TABLE IF NOT EXISTS orders_latest AS
SELECT
  id,
  customer_id,
  order_ts,
  amount,
  status,
  version,
  event_ts
FROM (
  SELECT
    *,
    ROW_NUMBER() OVER (PARTITION BY id ORDER BY version DESC) as rn
  FROM iceberg_orders_raw  -- Raw append-only table
)
WHERE rn = 1;  -- Only latest version
```

### Advantages
✅ Simple versioning logic
✅ Clear history (version=1,2,3...)
✅ Easy to time-travel

### Disadvantages
❌ Requires version field in source
❌ Extra computation for deduplication
❌ Doesn't handle deletes well

---

## 7. Choosing the Right Pattern

| Pattern | Complexity | Audit Trail | Deletes | Best For |
|---------|-----------|-------------|---------|----------|
| **Op Field** | Medium | ✅ Full | ✅ Native | External systems (SaaS APIs) |
| **Upsert** | Low | ❌ None | ❌ Can't | Simple duplicate handling |
| **Soft Delete** | Low | ✅ Marked | ✅ Marked | Compliance & reversibility |
| **Debezium** | High | ✅ Full | ✅ Native | Database change capture |
| **Versioning** | Medium | ✅ Full | ⚠️ Flag | Event-sourced systems |

### Recommendations by Source

| Source | Pattern | Example |
|--------|---------|---------|
| **HTTP API** | Op Field | Shopify webhook: includes `operation` in payload |
| **CSV/Batch** | Upsert | Daily customer snapshot: latest row per ID |
| **Database** | Debezium | PostgreSQL → Kafka → Iceberg (change log) |
| **Event Stream** | Versioning | Event-sourced app: each event has version |
| **Internal Queue** | Soft Delete | Your company's message queue: include `deleted` flag |

---

## Complete Example: Using Op Field with CDC Log

### Step 1: Kafka Events

```bash
# Producer sends events with 'op' field
cat <<'EOF' | docker-compose exec -T kafka kafka-console-producer \
  --bootstrap-server kafka:9092 --topic orders_cdc_topic
{"id":"1001","customer_id":"cust_1001","amount":15.50,"status":"created","op":"INSERT","ts":"2026-04-28T08:00:00"}
{"id":"1001","customer_id":"cust_1001","amount":15.50,"status":"in_progress","op":"UPDATE","ts":"2026-04-28T08:15:00"}
{"id":"1001","customer_id":"cust_1001","amount":15.50,"status":"completed","op":"UPDATE","ts":"2026-04-28T10:30:00"}
EOF
```

### Step 2: Flink SQL (Stream CDC Log)

```sql
SET 'execution.runtime-mode' = 'streaming';

CREATE TABLE orders_cdc (...);  -- See Pattern 1 above

-- Store raw CDC log
CREATE TABLE iceberg_orders_raw_log (...);

INSERT INTO iceberg_orders_raw_log
SELECT id, customer_id, order_ts, amount, status, op, ts
FROM orders_cdc;
```

### Step 3: dbt Materialized View (Final State)

```sql
-- dbt/models/mart_orders_current.sql
{{ config(materialized='view') }}

SELECT
  id,
  customer_id,
  order_ts,
  amount,
  status,
  latest_op_ts as last_modified_at
FROM (
  SELECT
    id,
    customer_id,
    order_ts,
    amount,
    status,
    ts as latest_op_ts,
    ROW_NUMBER() OVER (PARTITION BY id ORDER BY ts DESC) as rn
  FROM iceberg_catalog.`default`.iceberg_orders_raw_log
  WHERE op != 'DELETE'  -- Exclude deleted
)
WHERE rn = 1  -- Only latest
```

### Result

**Raw log table** (complete history):
```
id     op      status         ts
1001   INSERT  created        2026-04-28T08:00:00
1001   UPDATE  in_progress    2026-04-28T08:15:00
1001   UPDATE  completed      2026-04-28T10:30:00
```

**dbt View** (current state):
```
id     status      last_modified_at
1001   completed   2026-04-28T10:30:00
```

---

## Summary

**Use Pattern 1 (Op Field)** if:
- You control the event producer
- You need full audit trail
- You want to handle deletes

**Use Pattern 2 (Upsert)** if:
- Events naturally deduplicate
- You don't need history
- Fast ingestion matters

**Use Pattern 3 (Soft Delete)** if:
- You need reversible deletes
- Compliance/GDPR requires audit trail

**Use Pattern 4 (Debezium)** if:
- Capturing from existing database
- You want industry-standard CDC
- Need rich before/after metadata

**Use Pattern 5 (Versioning)** if:
- Source already has versions
- Event-sourced architecture
- Simple deduplication logic
