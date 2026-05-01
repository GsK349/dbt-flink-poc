# Streaming Pipeline Patterns: Upsert, Update, Delete, and Dedupe

This document explains the three CDC (Change Data Capture) streaming patterns implemented in this project, the exact mechanics behind each, how test data was produced, and what was run to prove correctness.

---

## Overview: Three Patterns, One Problem Space

When a source system emits a stream of events for the same record (create → update → delete), the downstream data lake must handle those changes. This project implements three distinct strategies, each with a different trade-off between simplicity, auditability, and recoverability.

```
Kafka Event Stream                    Iceberg Table (on S3)
──────────────────                    ─────────────────────
  id=1001 status=created   ──────▶   Pattern 1: UPSERT      → 1 row (latest state)
  id=1001 status=shipping  ──────▶   Pattern 2: CDC LOG      → N rows (full history)
  id=1001 status=delivered ──────▶   Pattern 3: SOFT DELETE  → 1 row (with is_deleted flag)
```

All three pipelines run simultaneously in Flink 1.20.1 as long-running streaming jobs.

---

## Pattern 1: Upsert with Primary Key Deduplication

### What It Does

Each Kafka message contains only the **current state** of a record (no operation type needed). When the same `id` arrives multiple times, Flink keeps only the **latest version** in Iceberg. Old values are permanently overwritten.

### How It Works — Step by Step

```
Kafka Topic: orders_upsert
────────────────────────────────────────────────────────────────────
Message 1:  {"id":"5001","status":"created","amount":99.00}
Message 2:  {"id":"5001","status":"in_progress","amount":99.00}
Message 3:  {"id":"5001","status":"completed","amount":99.00}
Message 4:  {"id":"5002","status":"created","amount":45.50}
```

**Flink processes these in order:**

1. Message 1 arrives → Flink sees id=5001, no existing row → **INSERT** into Iceberg
2. Message 2 arrives → Flink sees id=5001, row exists → **REPLACE** (Iceberg `rowDelta` with delete+insert)
3. Message 3 arrives → Flink sees id=5001, row exists → **REPLACE** again with final state
4. Message 4 arrives → Flink sees id=5002, no existing row → **INSERT** into Iceberg

**Result in Iceberg:**
```
┌──────┬────────────┬───────────┐
│  id  │   status   │  amount   │
├──────┼────────────┼───────────┤
│ 5001 │ completed  │ 99.00     │   ← only latest, 3 events collapsed to 1
│ 5002 │ created    │ 45.50     │
└──────┴────────────┴───────────┘
```

### The Key Iceberg Mechanics

Two table properties make this work:

```sql
PRIMARY KEY (id) NOT ENFORCED   -- tells Flink to route on id for dedup
'write.upsert.enabled' = 'true' -- tells Iceberg to write position-delete files
```

Without `write.upsert.enabled`, Flink cannot produce the Iceberg `RowDelta` commit format required for updates. With it, Iceberg commits two file operations atomically:
- A **data file** with the new row value
- A **position-delete file** marking the old row as deleted

This is why the table had to be **dropped and recreated** — you cannot add this property to an existing Iceberg table retroactively.

### SQL — What Was Run

```sql
-- File: sql/streaming_upsert_primary_key.sql

SET 'execution.runtime-mode' = 'streaming';
SET 'execution.checkpointing.interval' = '10s';
SET 'execution.checkpointing.mode' = 'EXACTLY_ONCE';

-- Kafka source (no PRIMARY KEY here — source is append-only)
CREATE TABLE IF NOT EXISTS orders_upsert (
  id STRING,
  customer_id STRING,
  order_ts TIMESTAMP(3),
  amount DECIMAL(10,2),
  status STRING
) WITH (
  'connector' = 'kafka',
  'topic' = 'orders_upsert',
  'properties.bootstrap.servers' = 'kafka:9092',
  'scan.startup.mode' = 'earliest-offset',
  'format' = 'json'
);

-- Iceberg sink with PRIMARY KEY and upsert enabled
CREATE TABLE IF NOT EXISTS iceberg_catalog.`default`.iceberg_orders_upsert (
  id STRING,
  customer_id STRING,
  order_ts TIMESTAMP(3),
  amount DECIMAL(10,2),
  status STRING,
  PRIMARY KEY (id) NOT ENFORCED
) WITH (
  'write.format.default' = 'parquet',
  'write.upsert.enabled' = 'true'
);

-- Long-running streaming job
INSERT INTO iceberg_catalog.`default`.iceberg_orders_upsert
SELECT id, customer_id, order_ts, amount, status
FROM default_catalog.default_database.orders_upsert;
```

### Test Data — What Was Produced

```bash
# Produced to Kafka topic 'orders_upsert':

# Event 1: Order 5001 created
kafka-console-producer --topic orders_upsert --bootstrap-server localhost:9092
{"id":"5001","customer_id":"cust_5001","order_ts":"2026-04-28T09:00:00","amount":99.00,"status":"created"}

# Event 2: Order 5001 updated
{"id":"5001","customer_id":"cust_5001","order_ts":"2026-04-28T09:00:00","amount":99.00,"status":"in_progress"}

# Event 3: Order 5001 completed
{"id":"5001","customer_id":"cust_5001","order_ts":"2026-04-28T09:00:00","amount":99.00,"status":"completed"}

# Event 4: New order 5002
{"id":"5002","customer_id":"cust_5002","order_ts":"2026-04-28T09:05:00","amount":45.50,"status":"created"}
```

### Proof — Verification Query

```sql
-- Run in batch mode via sql-client.sh -f
SET 'execution.runtime-mode' = 'batch';
SET 'sql-client.execution.result-mode' = 'TABLEAU';

SELECT id, status, order_ts FROM iceberg_catalog.`default`.iceberg_orders_upsert ORDER BY id;
```

**Actual output (verified):**
```
+------+-----------+----------------------------+
│  id  │  status   │          order_ts          │
+------+-----------+----------------------------+
│ 5001 │ completed │ 2026-04-28 09:00:00.000000 │
│ 5002 │ created   │ 2026-04-28 09:05:00.000000 │
+------+-----------+----------------------------+
2 rows in set (18.69 seconds)
```

**What this proves:**
- 3 events for id=5001 were **collapsed to 1 row** — deduplication works
- The final state (`completed`) is what's stored — upsert semantics are correct
- id=5002 has 1 row — normal insert works

### When to Use

| Use upsert when... | Do NOT use when... |
|--------------------|-------------------|
| Only the current state matters | You need to audit who changed what |
| Storage efficiency is important | Rollback or time-travel is needed |
| Source system has no delete events | DELETE operations must be tracked |

---

## Pattern 2: CDC Log (Append-Only Full History)

### What It Does

Every single event from the source system — INSERT, UPDATE, DELETE — is appended to an Iceberg table as a new row. Nothing is ever overwritten. The table grows continuously and is the **source of truth** for the full audit trail.

### How It Works — Step by Step

```
Kafka Topic: orders_cdc_op_body
────────────────────────────────────────────────────────────────────────────────────
Message 1:  {"id":"2001","status":"created","operation":"INSERT","event_ts":"T08:00"}
Message 2:  {"id":"2002","status":"created","operation":"INSERT","event_ts":"T08:05"}
Message 3:  {"id":"2001","status":"processing","operation":"UPDATE","event_ts":"T08:15"}
Message 4:  {"id":"2001","status":"completed","operation":"UPDATE","event_ts":"T10:30"}
```

**Flink processes these in order:**

1. Message 1 → **APPEND** row to Iceberg (INSERT event for 2001)
2. Message 2 → **APPEND** row to Iceberg (INSERT event for 2002)
3. Message 3 → **APPEND** row to Iceberg (UPDATE event for 2001)
4. Message 4 → **APPEND** row to Iceberg (UPDATE event for 2001)

**Result in Iceberg:**
```
┌──────┬────────────┬───────────┬──────────────────────┐
│  id  │   status   │ operation │    event_timestamp   │
├──────┼────────────┼───────────┼──────────────────────┤
│ 2001 │ created    │ INSERT    │ 2026-04-28T08:00:00Z │
│ 2002 │ created    │ INSERT    │ 2026-04-28T08:05:00Z │
│ 2001 │ processing │ UPDATE    │ 2026-04-28T08:15:00Z │
│ 2001 │ completed  │ UPDATE    │ 2026-04-28T10:30:00Z │
└──────┴────────────┴───────────┴──────────────────────┘
```

### Why Operation Is in the Message Body (Not Headers)

**Flink 1.20.x Bug:** The `METADATA FROM 'headers'` feature uses `MAP<VARBINARY, VARBINARY>` lookups. Direct key access (e.g., `headers[CAST('operation' AS BYTES)]`) always returns NULL in Flink 1.20.1, even when the key exists (confirmed via `MAP_KEYS()` which showed the keys were present). This is a Flink bug.

**Fix:** Move `operation`, `source`, and `event_ts` from Kafka headers into the **JSON message body**. The new topic is `orders_cdc_op_body` (renamed from `orders_cdc_headers`).

### The Key Iceberg Mechanics

The CDC log table has **no PRIMARY KEY and no `write.upsert.enabled`**. It is a pure append-only table:

```sql
-- No PRIMARY KEY → no deduplication → every row is kept
-- No write.upsert.enabled → Iceberg uses normal append commits
CREATE TABLE iceberg_orders_cdc_log (
  ...
  operation STRING,       -- tracks what happened (INSERT/UPDATE/DELETE)
  event_timestamp STRING, -- when it happened in the source system
  ingested_at TIMESTAMP   -- when Flink ingested it
) WITH (
  'write.format.default' = 'parquet'
  -- no write.upsert.enabled here
);
```

Flink adds `CURRENT_TIMESTAMP AS ingested_at` to each row so you can always distinguish source-event time from processing time.

### SQL — What Was Run

```sql
-- File: sql/streaming_cdc_op_field.sql

SET 'execution.runtime-mode' = 'streaming';
SET 'execution.checkpointing.interval' = '10s';

-- Kafka source: all CDC fields in the JSON body
CREATE TABLE IF NOT EXISTS orders_cdc_with_op (
  id STRING NOT NULL,
  customer_id STRING,
  order_ts TIMESTAMP(3),
  amount DECIMAL(10,2),
  status STRING,
  operation STRING,   -- "INSERT", "UPDATE", "DELETE"
  source STRING,
  event_ts STRING,    -- ISO-8601 timestamp from source system
  WATERMARK FOR order_ts AS order_ts - INTERVAL '0' SECOND
) WITH (
  'connector' = 'kafka',
  'topic' = 'orders_cdc_op_body',
  'properties.bootstrap.servers' = 'kafka:9092',
  'scan.startup.mode' = 'earliest-offset',
  'format' = 'json'
);

-- Append-only Iceberg table (no PRIMARY KEY, no upsert)
DROP TABLE IF EXISTS iceberg_catalog.`default`.iceberg_orders_cdc_log;
CREATE TABLE iceberg_catalog.`default`.iceberg_orders_cdc_log (
  id STRING,
  customer_id STRING,
  order_ts TIMESTAMP(3),
  amount DECIMAL(10,2),
  status STRING,
  operation STRING,
  source STRING,
  event_timestamp STRING,
  ingested_at TIMESTAMP(3)
) WITH (
  'write.format.default' = 'parquet'
);

-- Streaming insert: every event from Kafka → new row in Iceberg
INSERT INTO iceberg_catalog.`default`.iceberg_orders_cdc_log
SELECT
  id, customer_id, order_ts, amount, status,
  operation, source,
  event_ts AS event_timestamp,
  CURRENT_TIMESTAMP AS ingested_at
FROM default_catalog.default_database.orders_cdc_with_op;
```

### Test Data — What Was Produced

```bash
# Produced to Kafka topic 'orders_cdc_op_body':

# Event 1: Order 2001 created
{"id":"2001","customer_id":"cust_2001","order_ts":"2026-04-28T08:00:00","amount":25.00,
 "status":"created","operation":"INSERT","source":"order-service","event_ts":"2026-04-28T08:00:00Z"}

# Event 2: Order 2002 created
{"id":"2002","customer_id":"cust_2002","order_ts":"2026-04-28T08:05:00","amount":60.00,
 "status":"created","operation":"INSERT","source":"order-service","event_ts":"2026-04-28T08:05:00Z"}

# Event 3: Order 2001 updated
{"id":"2001","customer_id":"cust_2001","order_ts":"2026-04-28T08:00:00","amount":25.00,
 "status":"processing","operation":"UPDATE","source":"order-service","event_ts":"2026-04-28T08:15:00Z"}

# Event 4: Order 2001 completed
{"id":"2001","customer_id":"cust_2001","order_ts":"2026-04-28T08:00:00","amount":25.00,
 "status":"completed","operation":"UPDATE","source":"order-service","event_ts":"2026-04-28T10:30:00Z"}
```

### Proof — Verification Query

```sql
SET 'execution.runtime-mode' = 'batch';
SET 'sql-client.execution.result-mode' = 'TABLEAU';

SELECT id, status, operation, event_timestamp
FROM iceberg_catalog.`default`.iceberg_orders_cdc_log
ORDER BY event_timestamp;
```

**Actual output (verified):**
```
+------+------------+-----------+----------------------+
│  id  │   status   │ operation │    event_timestamp   │
+------+------------+-----------+----------------------+
│ 2001 │ created    │ INSERT    │ 2026-04-28T08:00:00Z │
│ 2002 │ created    │ INSERT    │ 2026-04-28T08:05:00Z │
│ 2001 │ processing │ UPDATE    │ 2026-04-28T08:15:00Z │
│ 2001 │ completed  │ UPDATE    │ 2026-04-28T10:30:00Z │
+------+------------+-----------+----------------------+
4 rows in set (14.11 seconds)
```

**What this proves:**
- All 4 events are stored — append-only, no deduplication
- `operation` column is populated (NOT NULL) — the Flink 1.20.x headers bug was fixed
- Events are ordered correctly by `event_timestamp`
- id=2001 has 3 rows (its full lifecycle) and id=2002 has 1 row

### dbt Layer on Top — mart_orders_cdc_current

The raw CDC log is the source for a dbt view that deduplicates by `ROW_NUMBER()` to produce the **current state** of each order:

```sql
-- File: dbt/models/mart_orders_cdc_current.sql
SELECT id, customer_id, order_ts, amount, status,
       latest_op as operation,
       latest_event_ts as last_modified_at
FROM (
  SELECT *,
    operation as latest_op,
    event_timestamp as latest_event_ts,
    ROW_NUMBER() OVER (PARTITION BY id ORDER BY event_timestamp DESC) as rn
  FROM iceberg_catalog.`default`.iceberg_orders_cdc_log
  WHERE operation <> 'DELETE'  -- exclude hard-deleted records
)
WHERE rn = 1  -- keep only the most recent event per id
```

This materializes as a Flink SQL view (not a physical table). When queried, it runs the deduplication logic on the live Iceberg data. Note that `!=` is not valid Flink SQL; the correct operator is `<>`.

### When to Use

| Use CDC log when... | Do NOT use when... |
|---------------------|-------------------|
| Full audit trail is required | Storage cost is a concern |
| You need to replay history | Latest-state-only queries are the norm |
| Compliance/GDPR tracking | Table will grow unboundedly |
| Time-travel analysis | |

---

## Pattern 3: Soft Delete with is_deleted Flag

### What It Does

Combines upsert semantics (PRIMARY KEY deduplication) with reversible deletion. When a DELETE event arrives, the row is not physically removed. Instead, it is **updated in-place** with `is_deleted = TRUE`. The data is preserved for compliance and audit, but downstream queries filter it out by default.

### How It Works — Step by Step

```
Kafka Topic: orders_soft_delete_body
────────────────────────────────────────────────────────────────────────────────────────
Message 1:  {"id":"4001","status":"created","is_deleted":false,"operation":"INSERT",...}
Message 2:  {"id":"4002","status":"created","is_deleted":false,"operation":"INSERT",...}
Message 3:  {"id":"4001","status":"processing","is_deleted":false,"operation":"UPDATE",...}
Message 4:  {"id":"4001","status":"cancelled","is_deleted":true,"operation":"DELETE",...}
```

**Flink processes these in order:**

1. Message 1 → INSERT 4001 with `is_deleted=false`
2. Message 2 → INSERT 4002 with `is_deleted=false`
3. Message 3 → UPSERT 4001 → row updated in-place, still `is_deleted=false`
4. Message 4 → UPSERT 4001 → row updated in-place, **`is_deleted=true`**, status=cancelled

**Result in Iceberg:**
```
┌──────┬───────────┬────────────┬───────────┐
│  id  │  status   │ is_deleted │ operation │
├──────┼───────────┼────────────┼───────────┤
│ 4001 │ cancelled │ TRUE       │ DELETE    │   ← physically present, logically deleted
│ 4002 │ created   │ FALSE      │ INSERT    │   ← active record
└──────┴───────────┴────────────┴───────────┘
```

### The Key Iceberg Mechanics

Like Pattern 1, this uses `PRIMARY KEY` + `write.upsert.enabled`. The difference is the `is_deleted` column which the application layer controls:

```sql
CREATE TABLE iceberg_orders_soft_delete_v3 (
  id STRING NOT NULL,
  ...
  is_deleted BOOLEAN,    -- TRUE = logically deleted, row preserved
  operation STRING,      -- DELETE event causes is_deleted=true via upsert
  PRIMARY KEY (id) NOT ENFORCED
) WITH (
  'write.format.default' = 'parquet',
  'write.upsert.enabled' = 'true'   -- enables in-place update semantics
);
```

When a DELETE event arrives with `is_deleted=true`, Flink performs the same Iceberg `RowDelta` commit as in Pattern 1 — it writes a new data file with `is_deleted=true` and a position-delete file for the old row. The row is not gone; it just has a new column value.

### Why "v3" in the Table Name

The soft delete table went through two iterations:
- **v1**: Attempted to read `operation` from Kafka headers → NULL bug (Flink 1.20.x)
- **v2**: Used `orders_soft_delete` topic — had wrong schema, no `write.upsert.enabled`
- **v3**: `orders_soft_delete_body` topic with all fields in JSON body, correct schema

### SQL — What Was Run

```sql
-- File: sql/streaming_soft_delete.sql

SET 'execution.runtime-mode' = 'streaming';
SET 'execution.checkpointing.interval' = '10s';
SET 'execution.checkpointing.mode' = 'EXACTLY_ONCE';

-- Kafka source: all fields in JSON body, including is_deleted and operation
CREATE TABLE IF NOT EXISTS orders_soft_delete_body (
  id STRING NOT NULL,
  customer_id STRING,
  order_ts TIMESTAMP(3),
  amount DECIMAL(10,2),
  status STRING,
  is_deleted BOOLEAN,   -- soft delete flag
  operation STRING,     -- INSERT / UPDATE / DELETE
  source STRING,
  event_ts STRING
) WITH (
  'connector' = 'kafka',
  'topic' = 'orders_soft_delete_body',
  'properties.bootstrap.servers' = 'kafka:9092',
  'scan.startup.mode' = 'earliest-offset',
  'format' = 'json'
);

-- Iceberg sink: upsert-enabled, PRIMARY KEY on id
DROP TABLE IF EXISTS iceberg_catalog.`default`.iceberg_orders_soft_delete_v3;
CREATE TABLE iceberg_catalog.`default`.iceberg_orders_soft_delete_v3 (
  id STRING NOT NULL,
  customer_id STRING,
  order_ts TIMESTAMP(3),
  amount DECIMAL(10,2),
  status STRING,
  is_deleted BOOLEAN,
  operation STRING,
  source STRING,
  event_timestamp STRING,
  PRIMARY KEY (id) NOT ENFORCED
) WITH (
  'write.format.default' = 'parquet',
  'write.upsert.enabled' = 'true'
);

-- Streaming job: every event either inserts or updates the row for that id
INSERT INTO iceberg_catalog.`default`.iceberg_orders_soft_delete_v3
SELECT id, customer_id, order_ts, amount, status,
       is_deleted, operation, source, event_ts AS event_timestamp
FROM default_catalog.default_database.orders_soft_delete_body;
```

### Test Data — What Was Produced

```bash
# Produced to Kafka topic 'orders_soft_delete_body':

# Event 1: Order 4001 created (active)
{"id":"4001","customer_id":"cust_4001","order_ts":"2026-04-28T10:00:00","amount":75.00,
 "status":"created","is_deleted":false,"operation":"INSERT","source":"order-service",
 "event_ts":"2026-04-28T10:00:00Z"}

# Event 2: Order 4002 created (active)
{"id":"4002","customer_id":"cust_4002","order_ts":"2026-04-28T10:05:00","amount":30.00,
 "status":"created","is_deleted":false,"operation":"INSERT","source":"order-service",
 "event_ts":"2026-04-28T10:05:00Z"}

# Event 3: Order 4001 updated (still active)
{"id":"4001","customer_id":"cust_4001","order_ts":"2026-04-28T10:00:00","amount":75.00,
 "status":"processing","is_deleted":false,"operation":"UPDATE","source":"order-service",
 "event_ts":"2026-04-28T11:00:00Z"}

# Event 4: Order 4001 soft-deleted (is_deleted=true)
{"id":"4001","customer_id":"cust_4001","order_ts":"2026-04-28T10:00:00","amount":75.00,
 "status":"cancelled","is_deleted":true,"operation":"DELETE","source":"order-service",
 "event_ts":"2026-04-28T12:00:00Z"}
```

### Proof — Verification Queries

**Raw Iceberg table (shows all rows including soft-deleted):**
```sql
SET 'execution.runtime-mode' = 'batch';
SET 'sql-client.execution.result-mode' = 'TABLEAU';

SELECT id, status, is_deleted, operation
FROM iceberg_catalog.`default`.iceberg_orders_soft_delete_v3
ORDER BY id;
```

**Actual output (verified):**
```
+------+-----------+------------+-----------+
│  id  │  status   │ is_deleted │ operation │
+------+-----------+------------+-----------+
│ 4001 │ cancelled │       TRUE │    DELETE │
│ 4002 │ created   │      FALSE │    INSERT │
+------+-----------+------------+-----------+
2 rows in set (17.10 seconds)
```

**What this proves:**
- 4 events for id=4001 were upserted to 1 row — dedup works
- `is_deleted=TRUE` for id=4001 — soft delete was applied
- id=4002 is still active (`is_deleted=FALSE`)
- The physical row for 4001 exists but is logically deleted

**dbt view — active orders only:**
```sql
-- File: dbt/models/mart_orders_active.sql
SELECT id, customer_id, order_ts, amount, status
FROM iceberg_catalog.`default`.iceberg_orders_soft_delete_v3
WHERE is_deleted = FALSE  -- hides soft-deleted records from consumers
```

This view was confirmed passing in the dbt run: `PASS=2 WARN=0 ERROR=0`.

### When to Use

| Use soft delete when... | Do NOT use when... |
|------------------------|-------------------|
| Data must be retained for compliance | True physical deletion is required (GDPR right-to-erasure) |
| Deletions should be reversible | Storage cost is critical |
| Downstream consumers must not see deleted records | Full audit trail with timestamps is needed (use CDC log instead) |

---

## How All Three Were Run Together

### Submitting the Streaming Jobs

Each SQL file creates its Kafka source, Iceberg table, and starts the INSERT job in one step:

```bash
# Run all 3 jobs in background (detached from terminal)
docker exec -d flink_dbt_poc-jobmanager-1 \
  /opt/flink/bin/sql-client.sh -f /tmp/streaming_upsert_primary_key.sql

docker exec -d flink_dbt_poc-jobmanager-1 \
  /opt/flink/bin/sql-client.sh -f /tmp/streaming_cdc_op_field.sql

docker exec -d flink_dbt_poc-jobmanager-1 \
  /opt/flink/bin/sql-client.sh -f /tmp/streaming_soft_delete.sql
```

The `-d` flag runs in background. Each command starts a sql-client process that submits the job to the Flink cluster and exits. The Flink job itself continues running in the cluster.

### Verifying Jobs Are Running

```bash
curl -s http://localhost:8081/jobs | python3 -c "
import json, sys
for j in json.load(sys.stdin)['jobs']:
    if j['status'] == 'RUNNING':
        print(j['id'], j['status'])
"
```

Expected: 3 running jobs, one per pipeline.

### Verifying Checkpointing

Each job has checkpointing enabled (`EXACTLY_ONCE` every 10 seconds). Check the Flink UI at `http://localhost:8081` → Jobs → select a job → Checkpoints tab. A successful checkpoint means Flink committed the current state to Iceberg and the job can recover from that point if it crashes.

Alternatively, check TaskManager logs:
```bash
docker logs flink_dbt_poc-taskmanager-1 2>&1 | grep "Checkpoint"
# Expected: "Checkpoint N completed" lines
```

### Running dbt on the Verified Data

```bash
# From project root, with SQL Gateway running on :8083
.venv39/bin/dbt run --profiles-dir dbt --select mart_orders_cdc_current mart_orders_active
```

**Actual dbt output:**
```
Running with dbt=1.3.7
Found 4 models ...

1 of 2 OK created sql view model mart_orders_active .............. [FINISHED in 4.52s]
2 of 2 OK created sql view model mart_orders_cdc_current .......... [FINISHED in 4.88s]

Completed successfully
Done. PASS=2 WARN=0 ERROR=0 SKIP=0 TOTAL=2
```

---

## Side-by-Side Comparison

| Dimension | Pattern 1: Upsert | Pattern 2: CDC Log | Pattern 3: Soft Delete |
|-----------|-------------------|--------------------|------------------------|
| **Iceberg rows per ID** | 1 (latest) | N (all events) | 1 (latest, with flag) |
| **PRIMARY KEY** | Yes | No | Yes |
| **write.upsert.enabled** | Yes | No | Yes |
| **DELETE handling** | Row gone | Row appended with op=DELETE | Row stays, is_deleted=TRUE |
| **Storage growth** | Bounded | Unbounded | Bounded |
| **Audit capability** | None | Full | Partial (latest state only) |
| **Kafka topic** | `orders_upsert` | `orders_cdc_op_body` | `orders_soft_delete_body` |
| **Iceberg table** | `iceberg_orders_upsert` | `iceberg_orders_cdc_log` | `iceberg_orders_soft_delete_v3` |
| **Flink Job ID** | `9cd5f820...` | `b663da83...` | `7cc82db6...` |
| **dbt model** | — | `mart_orders_cdc_current` | `mart_orders_active` |

---

## Common Pitfalls and How They Were Solved

### 1. Flink 1.20.x MAP Header Bug
**Problem:** `headers[CAST('operation' AS BYTES)]` always returns NULL.
**Solution:** Moved `operation`, `source`, `event_ts` into the JSON message body. Switched Kafka topics from `orders_cdc_headers` → `orders_cdc_op_body` and `orders_soft_delete` → `orders_soft_delete_body`.

### 2. Missing `write.upsert.enabled`
**Problem:** Without this property, Flink cannot write Iceberg `RowDelta` commits, causing upsert jobs to fail or produce duplicates.
**Solution:** Add `'write.upsert.enabled' = 'true'` to table options. **Existing tables must be dropped and recreated** — this property cannot be added after table creation.

### 3. `!=` Operator Not Supported in Flink SQL
**Problem:** `WHERE operation != 'DELETE'` raises `SqlParserException: Bang equal '!='  is not allowed`.
**Solution:** Use `<>` (ANSI SQL standard): `WHERE operation <> 'DELETE'`.

### 4. `!=` vs `<>` in dbt Models
**Same as above.** The dbt-flink adapter submits SQL to the Flink SQL Gateway, which enforces strict ANSI conformance. Any `!=` in a dbt model will fail.

### 5. AWS Credentials Lost on Container Restart
**Problem:** `docker compose up` reads env vars from the host shell. If credentials are not exported, containers start with empty AWS vars and all Iceberg/Glue operations fail with `SdkClientException: Unable to load credentials`.
**Solution:** Always export credentials before running docker compose:
```bash
export AWS_ACCESS_KEY_ID=$(grep aws_access_key_id ~/.aws/credentials | head -1 | awk '{print $3}')
export AWS_SECRET_ACCESS_KEY=$(grep aws_secret_access_key ~/.aws/credentials | head -1 | awk '{print $3}')
docker compose up -d
```

### 6. Stale dbt Session File
**Problem:** When the SQL Gateway container is recreated, old session IDs become invalid. dbt caches the session handle in `~/.dbt/flink-session.yml` and reuses it on the next run, causing `Session 'xxx' does not exist` errors.
**Solution:**
```bash
rm ~/.dbt/flink-session.yml
```

### 7. TaskManager Slot Exhaustion
**Problem:** With 4 TaskManager slots and 3 streaming jobs (each using slots), batch SELECT queries get stuck in `CREATED` state with no available slots.
**Solution:** Either add more TaskManager slots in `docker-compose.yml` (increase `taskmanager.numberOfTaskSlots`) or cancel one streaming job temporarily for batch verification. Batch SELECT jobs typically need 1 slot.

---

## Infrastructure Notes

| Component | Config |
|-----------|--------|
| Flink version | 1.20.1 |
| Iceberg version | 1.10.1 |
| Catalog | AWS Glue |
| Storage | S3 (`s3://flink-iceberg-warehouse/`) |
| Checkpointing | EXACTLY_ONCE, 10-second interval |
| TaskManager slots | 4 total |
| SQL Client | `sql-client.sh -f <file>` (interactive gateway too unstable) |
| SQL Gateway | Port 8083, separate Docker container |
| dbt adapter | dbt-flink 1.3.7 |
