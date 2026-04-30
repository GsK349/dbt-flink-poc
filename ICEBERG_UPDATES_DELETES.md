# Iceberg UPDATE and DELETE Operations — Complete Guide

This guide explains how to modify existing data in Iceberg tables, handle concurrent updates with streaming jobs, and understand the performance implications.

---

## Table of Contents

1. [Why Iceberg Supports Updates/Deletes](#1-why-iceberg-supports-updatesdeletes)
2. [Basic UPDATE Operations](#2-basic-update-operations)
3. [Basic DELETE Operations](#3-basic-delete-operations)
4. [Handling Duplicates and Corrections](#4-handling-duplicates-and-corrections)
5. [Concurrent Updates with Streaming](#5-concurrent-updates-with-streaming)
6. [Performance Implications](#6-performance-implications)
7. [Common Patterns](#7-common-patterns)
8. [Limitations and Workarounds](#8-limitations-and-workarounds)

---

## 1. Why Iceberg Supports Updates/Deletes

### Traditional Data Lakes (S3 + Hive)
```
❌ Can't update a row without rewriting entire files
❌ No transactions (concurrent writes corrupt data)
❌ Deleting a row requires full table rewrite
❌ No isolation (readers see partial updates)
```

### Apache Iceberg
```
✅ Updates use copy-on-write (efficient file management)
✅ Full ACID transactions (concurrent safety)
✅ Deletes are tracked without rewriting data
✅ Snapshot isolation (readers always see consistent state)
✅ Time travel (query historical snapshots)
```

### The Key Difference: Snapshots

Each operation (INSERT, UPDATE, DELETE) creates a new snapshot:

```
Snapshot 0 (Initial):     id=1001, status='created'
  ↓ UPDATE status='completed' WHERE id=1001
Snapshot 1:               id=1001, status='completed'
  ↓ DELETE WHERE id=1001
Snapshot 2:               (no rows)
  ↓ INSERT new id=1002, status='created'
Snapshot 3:               id=1002, status='created'
```

Readers always read one consistent snapshot. Writers create new snapshots without blocking readers.

---

## 2. Basic UPDATE Operations

### Syntax

```sql
UPDATE iceberg_catalog.`default`.iceberg_orders
SET status = 'completed'
WHERE id = '1001';
```

### Example: Fix an order status

```sql
-- One order went wrong, fix it
UPDATE iceberg_catalog.`default`.iceberg_orders
SET status = 'cancelled', amount = 0.00
WHERE id = '1001' AND customer_id = 'cust_1001';
```

### Example: Bulk status change

```sql
-- All April orders are now verified
UPDATE iceberg_catalog.`default`.iceberg_orders
SET status = 'verified'
WHERE order_ts >= '2026-04-01' AND order_ts < '2026-05-01';
```

### What Happens Under the Hood

```
1. Read all data files that might contain matching rows
2. Find rows where status != 'completed'
3. Rewrite those files with updated values
4. Mark old files as deleted (in metadata)
5. Create new snapshot pointing to new files
6. Commit transaction atomically
```

**Key point**: Only files containing matching rows are rewritten. If your table is partitioned by date and you update only April data, May files are untouched.

### Testing UPDATE in SQL

```bash
docker exec flink_dbt_poc-jobmanager-1 \
  /opt/flink/bin/sql-client.sh << 'EOF'

SET 'execution.runtime-mode' = 'batch';

CREATE CATALOG IF NOT EXISTS iceberg_catalog WITH (
  'type' = 'iceberg',
  'warehouse' = 's3://flink-iceberg-warehouse/',
  'catalog-impl' = 'org.apache.iceberg.aws.glue.GlueCatalog',
  'io-impl' = 'org.apache.iceberg.aws.s3.S3FileIO'
);

-- Check current data
SELECT id, status, amount FROM iceberg_catalog.`default`.iceberg_orders LIMIT 5;

-- Update one row
UPDATE iceberg_catalog.`default`.iceberg_orders
SET status = 'completed'
WHERE id = '1001';

-- Verify the update
SELECT id, status, amount FROM iceberg_catalog.`default`.iceberg_orders WHERE id = '1001';

EOF
```

---

## 3. Basic DELETE Operations

### Syntax

```sql
DELETE FROM iceberg_catalog.`default`.iceberg_orders
WHERE id = '1001';
```

### Example: Remove cancelled orders

```sql
-- Purge all cancelled orders from 2026-04
DELETE FROM iceberg_catalog.`default`.iceberg_orders
WHERE status = 'cancelled' 
  AND order_ts >= '2026-04-01' 
  AND order_ts < '2026-05-01';
```

### Example: Delete by customer

```sql
-- Remove all orders from a specific customer (GDPR right to be forgotten)
DELETE FROM iceberg_catalog.`default`.iceberg_orders
WHERE customer_id = 'cust_1001';
```

### What Happens Under the Hood

```
Option 1: Copy-on-Write (Default in Flink)
  1. Read files containing matching rows
  2. Rewrite without those rows
  3. Mark old files as deleted
  4. New snapshot points to new files

Option 2: Merge-on-Read (Advanced, not shown here)
  1. Record deletions in a delete file
  2. Don't rewrite data files
  3. Readers skip deleted rows at query time
  (Better for frequent deletes, worse for query performance)
```

### Testing DELETE in SQL

```bash
docker exec flink_dbt_poc-jobmanager-1 \
  /opt/flink/bin/sql-client.sh << 'EOF'

SET 'execution.runtime-mode' = 'batch';

CREATE CATALOG IF NOT EXISTS iceberg_catalog WITH (
  'type' = 'iceberg',
  'warehouse' = 's3://flink-iceberg-warehouse/',
  'catalog-impl' = 'org.apache.iceberg.aws.glue.GlueCatalog',
  'io-impl' = 'org.apache.iceberg.aws.s3.S3FileIO'
);

-- Count before
SELECT COUNT(*) as total_orders FROM iceberg_catalog.`default`.iceberg_orders;

-- Delete one row
DELETE FROM iceberg_catalog.`default`.iceberg_orders WHERE id = '1001';

-- Count after (should be 1 less)
SELECT COUNT(*) as total_orders FROM iceberg_catalog.`default`.iceberg_orders;

EOF
```

---

## 4. Handling Duplicates and Corrections

### Real-World Scenario

Your streaming job ran for a week. Later you discover:
- Order #1001 was duplicated (same order in Iceberg twice)
- Order #1002 has wrong customer_id (should be 'cust_2002' not 'cust_2001')

### Solution 1: Delete Duplicates

```sql
-- Find duplicates
SELECT id, COUNT(*) as count FROM iceberg_catalog.`default`.iceberg_orders
GROUP BY id
HAVING COUNT(*) > 1;

-- Delete one copy of the duplicate
DELETE FROM iceberg_catalog.`default`.iceberg_orders
WHERE id = '1001' AND customer_id = 'cust_1001' LIMIT 1;
```

⚠️ **Note**: `LIMIT` in DELETE may not be supported in all Flink versions. Alternative:

```sql
-- Delete based on Iceberg's internal row number
DELETE FROM iceberg_catalog.`default`.iceberg_orders
WHERE id = '1001' 
  AND customer_id = 'cust_1001'
  AND order_ts = '2026-04-28T08:00:00';
```

### Solution 2: Fix Incorrect Data

```sql
-- Correct the customer_id
UPDATE iceberg_catalog.`default`.iceberg_orders
SET customer_id = 'cust_2002'
WHERE id = '1002' AND customer_id = 'cust_2001';
```

### Solution 3: Merge (Insert or Update)

Iceberg supports a MERGE operation that's like SQL UPSERT:

```sql
MERGE INTO iceberg_catalog.`default`.iceberg_orders AS t
USING (
  SELECT '1002' as id, 'cust_2002' as customer_id, 
         TIMESTAMP '2026-04-28 08:05:00' as order_ts,
         42.00 as amount, 'completed' as status
) AS s
ON t.id = s.id
WHEN MATCHED THEN
  UPDATE SET 
    customer_id = s.customer_id,
    status = s.status,
    amount = s.amount
WHEN NOT MATCHED THEN
  INSERT (id, customer_id, order_ts, amount, status)
  VALUES (s.id, s.customer_id, s.order_ts, s.amount, s.status);
```

⚠️ **Check Flink version**: MERGE may require Flink 1.18+. Verify:

```bash
docker exec flink_dbt_poc-jobmanager-1 \
  /opt/flink/bin/sql-client.sh -e "SELECT FLINK_VERSION();"
```

---

## 5. Concurrent Updates with Streaming

### The Challenge

You have **two jobs writing to the same table**:
- Streaming job: Kafka → Iceberg (runs 24/7)
- Manual update job: Fix data errors

```
08:00 Streaming job writes: id=1001, status='created'
08:05 Manual job updates: UPDATE status='completed' WHERE id=1001
08:06 Streaming job writes: id=1001, status='created' again (replay!)
      ↑ Problem: Undo the manual fix!
```

### Solution 1: Pause Streaming Before Updates

```bash
# On Flink Web UI (http://localhost:8081):
# 1. Find the streaming job
# 2. Click "Cancel"
# 3. Run your UPDATE/DELETE operations
# 4. Restart the streaming job (from checkpoint)
```

The streaming job resumes from its last checkpoint, skipping replayed data.

### Solution 2: Use Kafka Consumer Offset Management

Prevent the streaming job from replaying:

```sql
-- Check current Kafka offset
-- (This is automatic in Flink, but you can inspect it)

-- Stop the streaming job at a known good offset
-- Then run updates
-- Then restart streaming from that offset onwards
```

### Solution 3: Separate Tables by Phase

```
Iceberg table structure:
├── iceberg_orders_raw   (streaming writes here, never update)
├── iceberg_orders_clean (UPDATE/DELETE operations here)
└── iceberg_orders_final (dbt reads from here, materialized view)
```

Flow:
```
Streaming job:
  Kafka → iceberg_orders_raw

Batch job (hourly):
  iceberg_orders_raw → iceberg_orders_clean (with fixes/deduplication)

dbt models:
  iceberg_orders_clean → iceberg_orders_final views
```

### Recommended Architecture

```
┌─────────────────────────────────────────┐
│ Kafka: orders_topic                     │
└────────────────┬────────────────────────┘
                 │
                 ├─────────────────────────────────┐
                 ↓                                 ↓
        ┌─────────────────┐         ┌──────────────────────┐
        │ Streaming Job   │         │ Manual Operations    │
        │ (runs 24/7)     │         │ (on-demand)          │
        └────────┬────────┘         └──────────────┬───────┘
                 │                                  │
                 ↓                                  ↓
        ┌─────────────────────────────────────────┐
        │ Iceberg: iceberg_orders (APPEND ONLY)   │
        │ ✓ Streaming inserts                     │
        │ ✓ Manual updates/deletes                │
        │ (Both are ACID-safe, no conflicts)      │
        └────────────┬────────────────────────────┘
                     │
                     ↓
        ┌─────────────────────────────────────────┐
        │ dbt Models: stg_orders, mart_orders     │
        │ (Read from iceberg_orders)              │
        └─────────────────────────────────────────┘
```

**Key insight**: Iceberg's ACID transactions ensure that even with concurrent writes, data is never corrupted. Both streaming inserts and manual updates are safe to do simultaneously.

---

## 6. Performance Implications

### UPDATE/DELETE Costs

| Operation | Cost | Time | When to Use |
|-----------|------|------|------------|
| INSERT (append) | Cheap | Fast | New data |
| UPDATE (few rows) | Medium | Moderate | Fix errors, status changes |
| UPDATE (many rows) | Expensive | Slow | Avoid bulk updates |
| DELETE (few rows) | Medium | Moderate | Remove spam/GDPR |
| DELETE (many rows) | Expensive | Slow | Avoid; use new table instead |

### Example: Cost Comparison

```
Table: iceberg_orders
- 1 million rows
- Stored in 100 parquet files (1MB each)
- Partitioned: year/month/day

Scenario 1: Fix one order status
  UPDATE ... WHERE id = '1001'
  → Only 1 file needs rewriting (assuming year/month/day partition)
  → Time: <1 second
  ✅ Good use case

Scenario 2: Mark all April orders as 'verified'
  UPDATE ... WHERE order_ts >= '2026-04-01'
  → ~30 files need rewriting (one per April day)
  → Time: ~5-10 seconds
  ✅ Acceptable, but consider batching

Scenario 3: Delete all cancelled orders (1% of table)
  DELETE WHERE status = 'cancelled'
  → Potentially all 100 files need rewriting
  → Time: ~30-60 seconds
  ⚠️ Slow; consider if necessary

Scenario 4: Recreate table with cleaner data
  1. INSERT INTO iceberg_orders_v2 SELECT * FROM iceberg_orders WHERE status != 'cancelled'
  2. Swap tables
  → Time: ~2-5 seconds
  ✅ Faster than DELETE for large operations
```

### Optimization: Partition Pruning

```sql
-- ❌ SLOW: Touches all files
UPDATE iceberg_catalog.`default`.iceberg_orders
SET amount = amount * 1.1;

-- ✅ FAST: Only touches April files
UPDATE iceberg_catalog.`default`.iceberg_orders
SET amount = amount * 1.1
WHERE order_ts >= '2026-04-01' AND order_ts < '2026-05-01';
```

Always include partition column in WHERE clause for updates/deletes.

---

## 7. Common Patterns

### Pattern 1: Fix Duplicates from Failed Streaming

```sql
-- Find duplicates
WITH duplicates AS (
  SELECT id, customer_id, order_ts, COUNT(*) as cnt
  FROM iceberg_catalog.`default`.iceberg_orders
  GROUP BY id, customer_id, order_ts
  HAVING COUNT(*) > 1
)
SELECT * FROM iceberg_catalog.`default`.iceberg_orders
WHERE (id, customer_id, order_ts) IN (SELECT id, customer_id, order_ts FROM duplicates);

-- Delete extra copies (keep oldest by file)
DELETE FROM iceberg_catalog.`default`.iceberg_orders
WHERE id IN (
  SELECT id FROM iceberg_catalog.`default`.iceberg_orders
  GROUP BY id
  HAVING COUNT(*) > 1
)
-- Keep one copy (this is a simplification)
AND order_ts != (
  SELECT MIN(order_ts) FROM iceberg_catalog.`default`.iceberg_orders
  WHERE id = iceberg_orders.id
);
```

### Pattern 2: Data Quality Corrections

```sql
-- Correct orders with obviously wrong amounts
UPDATE iceberg_catalog.`default`.iceberg_orders
SET amount = 100.00
WHERE amount < 0 OR amount > 100000;
```

### Pattern 3: GDPR Right to be Forgotten

```sql
-- Delete all customer data on request
DELETE FROM iceberg_catalog.`default`.iceberg_orders
WHERE customer_id = 'cust_request_to_delete';
```

### Pattern 4: Time-based Purge

```sql
-- Archive old data (delete from hot table, keep in archive)
DELETE FROM iceberg_catalog.`default`.iceberg_orders
WHERE order_ts < '2025-01-01';  -- Keep only 2025+

-- Before deleting, copy to archive:
-- INSERT INTO iceberg_orders_archive_2024
-- SELECT * FROM iceberg_orders WHERE order_ts < '2025-01-01'
```

---

## 8. Limitations and Workarounds

### Limitation 1: UPDATE/DELETE in Streaming Mode

❌ **Not allowed**: UPDATE/DELETE while `execution.runtime-mode = 'streaming'`

```sql
-- This FAILS in streaming mode:
SET 'execution.runtime-mode' = 'streaming';
UPDATE iceberg_catalog.`default`.iceberg_orders SET status = 'verified';
```

**Workaround**: Use batch mode for modifications

```sql
SET 'execution.runtime-mode' = 'batch';
UPDATE iceberg_catalog.`default`.iceberg_orders SET status = 'verified';
```

### Limitation 2: Cannot UPDATE Partition Columns

❌ **Not allowed**: Change the partition column value

```sql
-- This FAILS:
UPDATE iceberg_catalog.`default`.iceberg_orders
SET year = 2025  -- Can't change partition column!
WHERE id = '1001';
```

**Workaround**: Delete + re-insert with new partition value

```sql
DELETE FROM iceberg_catalog.`default`.iceberg_orders
WHERE id = '1001';

INSERT INTO iceberg_catalog.`default`.iceberg_orders
VALUES ('1001', 'cust_1001', ..., 2025, ...);
```

### Limitation 3: MERGE Support Varies by Flink Version

Some Flink versions don't support MERGE. Check:

```bash
docker exec flink_dbt_poc-jobmanager-1 \
  /opt/flink/bin/sql-client.sh << 'EOF'
SET 'execution.runtime-mode' = 'batch';
CREATE CATALOG IF NOT EXISTS iceberg_catalog WITH (...);
MERGE INTO iceberg_catalog.`default`.iceberg_orders ...
EOF
```

If MERGE fails, use separate UPDATE/DELETE or insert into a temp table and swap.

---

## Practical Example: Complete Workflow

### Step 1: Check Current Data

```sql
SELECT id, customer_id, status, amount FROM iceberg_catalog.`default`.iceberg_orders
LIMIT 10;
```

Output:
```
id    customer_id  status     amount
1001  cust_1001    created    15.50
1002  cust_1002    completed  42.00
1003  cust_1003    cancelled  9.99
1001  cust_1001    created    15.50  ← Duplicate!
```

### Step 2: Find Issues

```sql
-- Find duplicates
SELECT id, COUNT(*) as cnt FROM iceberg_catalog.`default`.iceberg_orders
GROUP BY id
HAVING COUNT(*) > 1;

-- Find bad data
SELECT * FROM iceberg_catalog.`default`.iceberg_orders
WHERE amount < 0 OR amount > 100000;
```

### Step 3: Run Fixes

```sql
-- Delete duplicates
DELETE FROM iceberg_catalog.`default`.iceberg_orders
WHERE id = '1001' AND customer_id = 'cust_1001'
  AND order_ts = '2026-04-28T08:00:00';

-- Update status
UPDATE iceberg_catalog.`default`.iceberg_orders
SET status = 'verified'
WHERE id = '1002';

-- Fix wrong amounts
UPDATE iceberg_catalog.`default`.iceberg_orders
SET amount = CASE
  WHEN amount < 0 THEN 0
  WHEN amount > 100000 THEN 99999.99
  ELSE amount
END
WHERE amount < 0 OR amount > 100000;
```

### Step 4: Verify

```sql
SELECT id, COUNT(*) as cnt FROM iceberg_catalog.`default`.iceberg_orders
GROUP BY id;  -- Should have no duplicates

SELECT * FROM iceberg_catalog.`default`.iceberg_orders
WHERE amount < 0 OR amount > 100000;  -- Should be empty
```

---

## Summary

| Need | Solution |
|------|----------|
| Fix one order | `UPDATE ... WHERE id = '1001'` |
| Delete spam | `DELETE ... WHERE status = 'spam'` |
| Deduplicate | Find duplicates, `DELETE` extra copies |
| Bulk corrections | `UPDATE ... WHERE condition` (with partition filter) |
| Upsert | `MERGE INTO ... WHEN MATCHED/NOT MATCHED` |
| Keep history | Use Iceberg time travel: `SELECT * FROM table FOR SYSTEM_TIME AS OF '2026-04-28'` |
| Concurrent updates | Use ACID transactions (automatic) |
| Archive old data | `DELETE` after copying to archive table |

**Best practice**: Keep your raw Iceberg table append-only (streaming writes), and do all corrections via a separate batch job that writes to a cleaned table used by dbt.
