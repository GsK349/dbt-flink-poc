# GDPR & Data Retention with Iceberg — Best Practices

This guide covers handling data retention policies, GDPR right-to-be-forgotten, and compliant data management with Iceberg.

---

## Table of Contents

1. [GDPR Right to be Forgotten](#1-gdpr-right-to-be-forgotten)
2. [Data Retention Policies](#2-data-retention-policies)
3. [Handling Audit Trails](#3-handling-audit-trails)
4. [Combining Streaming + Deletion](#4-combining-streaming--deletion)
5. [Compliance Patterns](#5-compliance-patterns)

---

## 1. GDPR Right to be Forgotten

### What It Means
Under GDPR, customers can request that you delete all personal data about them. This must happen within 30 days of request.

### Implementation

**Step 1: Receive deletion request**
```
Customer requests: "Delete all my data"
You identify: customer_id = 'cust_1001'
```

**Step 2: Archive before deletion (optional but recommended)**
```sql
SET 'execution.runtime-mode' = 'batch';

CREATE CATALOG IF NOT EXISTS iceberg_catalog WITH (
  'type' = 'iceberg',
  'warehouse' = 's3://flink-iceberg-warehouse/',
  'catalog-impl' = 'org.apache.iceberg.aws.glue.GlueCatalog',
  'io-impl' = 'org.apache.iceberg.aws.s3.S3FileIO'
);

USE CATALOG iceberg_catalog;
USE `default`;

-- Archive the customer's data (legal requirement: keep 7 years for audit)
INSERT INTO iceberg_orders_archived_for_gdpr
SELECT * FROM iceberg_orders WHERE customer_id = 'cust_1001';
```

**Step 3: Delete from active table**
```sql
-- Perform the actual deletion
DELETE FROM iceberg_orders
WHERE customer_id = 'cust_1001';

-- Verify deletion
SELECT COUNT(*) FROM iceberg_orders WHERE customer_id = 'cust_1001';
-- Should return 0
```

**Step 4: Document the deletion**
```
Deletion record:
  Customer ID: cust_1001
  Deletion date: 2026-05-01
  Deletion reason: GDPR Right to be Forgotten
  Archived to: iceberg_orders_archived_for_gdpr (snapshot v45)
  Confirmation: All 5 orders deleted from active table
```

### Automating Deletion Requests

```sql
-- Create a dedicated table to track deletion requests
CREATE TABLE IF NOT EXISTS deletion_requests (
  request_id STRING,
  customer_id STRING,
  request_date TIMESTAMP(3),
  status STRING,  -- pending, completed, failed
  deleted_row_count BIGINT
);

-- Process pending requests (batch job, runs daily)
SET 'execution.runtime-mode' = 'batch';

INSERT INTO deletion_requests (request_id, customer_id, request_date, status, deleted_row_count)
SELECT 
  request_id, 
  customer_id, 
  request_date,
  'completed',
  COUNT(*) as deleted_count
FROM deletion_requests
WHERE status = 'pending'
  AND DATE_FORMAT(request_date, 'yyyy-MM-dd') = DATE_FORMAT(CURRENT_TIMESTAMP, 'yyyy-MM-dd');

-- Then execute the actual deletion:
DELETE FROM iceberg_orders
WHERE customer_id IN (
  SELECT customer_id FROM deletion_requests WHERE status = 'pending'
);
```

---

## 2. Data Retention Policies

### Policy: Keep 2 Years of Order Data

```sql
SET 'execution.runtime-mode' = 'batch';

-- Run this monthly to archive old data
INSERT INTO iceberg_orders_archive_2024
SELECT * FROM iceberg_orders
WHERE order_ts >= TIMESTAMP '2024-01-01'
  AND order_ts < TIMESTAMP '2025-01-01';

-- Delete from active table (keep only 2 years)
DELETE FROM iceberg_orders
WHERE order_ts < TIMESTAMP '2024-01-01';

-- Verify retention
SELECT 
  EXTRACT(YEAR FROM order_ts) as year,
  COUNT(*) as count
FROM iceberg_orders
GROUP BY EXTRACT(YEAR FROM order_ts)
ORDER BY year DESC;
```

### Policy: PII Expiration (Pseudonymization)

Customers accept that you keep their orders but want personal info deleted after 1 year:

```sql
-- Pseudonymize customer_id (replace with hash) for orders older than 1 year
UPDATE iceberg_orders
SET customer_id = CONCAT('HASH_', MD5(customer_id))
WHERE order_ts < CURRENT_TIMESTAMP - INTERVAL '1' YEAR;

-- Now customer_id is no longer personally identifiable
```

### Policy: Automatic Archival

```sql
-- Create a "cold" archive table for old data
CREATE TABLE IF NOT EXISTS iceberg_orders_archive (
  id STRING,
  customer_id STRING,
  order_ts TIMESTAMP(3),
  amount DECIMAL(10,2),
  status STRING,
  year INT,
  month INT
)
WITH (
  'write.format.default' = 'parquet'
);

-- Monthly job: move data older than 1 year to archive
BEGIN
  INSERT INTO iceberg_orders_archive
  SELECT *, 
    EXTRACT(YEAR FROM order_ts) as year,
    EXTRACT(MONTH FROM order_ts) as month
  FROM iceberg_orders
  WHERE order_ts < CURRENT_TIMESTAMP - INTERVAL '1' YEAR
    AND id NOT IN (SELECT id FROM iceberg_orders_archive);  -- Prevent duplicates

  DELETE FROM iceberg_orders
  WHERE order_ts < CURRENT_TIMESTAMP - INTERVAL '1' YEAR;
END;
```

---

## 3. Handling Audit Trails

### Immutable Audit Log Pattern

Never modify your audit log. Instead, create new records:

```sql
-- Immutable log of all changes (append-only)
CREATE TABLE IF NOT EXISTS iceberg_orders_audit (
  event_id STRING,
  event_type STRING,  -- INSERT, UPDATE, DELETE, CUSTOMER_DELETED
  order_id STRING,
  customer_id STRING,
  old_status STRING,
  new_status STRING,
  event_timestamp TIMESTAMP(3),
  event_reason STRING  -- why the change happened
);

-- When you DELETE for GDPR, log it
INSERT INTO iceberg_orders_audit
VALUES (
  'evt_12345',
  'CUSTOMER_DELETED',
  NULL,  -- All orders for this customer
  'cust_1001',
  NULL,
  NULL,
  CURRENT_TIMESTAMP,
  'GDPR Article 17 Right to be Forgotten request #REQ-2026-0501'
);
```

### Time Travel with Audit Log

```sql
-- Reconstruct customer's data as it was on April 28
SELECT * FROM iceberg_orders
  FOR SYSTEM_TIME AS OF '2026-04-28'
WHERE customer_id = 'cust_1001';

-- Combined with audit log:
SELECT 
  a.event_type,
  a.event_timestamp,
  a.old_status,
  a.new_status
FROM iceberg_orders_audit a
WHERE a.customer_id = 'cust_1001'
ORDER BY a.event_timestamp;
-- Provides complete change history
```

---

## 4. Combining Streaming + Deletion

### Safe Deletion Pattern

Problem: Streaming job keeps writing while you're deleting.

Solution: Use a "soft delete" flag instead of hard deletes:

```sql
-- Add an is_deleted flag instead of actual DELETE
ALTER TABLE iceberg_orders ADD COLUMN is_deleted BOOLEAN DEFAULT FALSE;

-- "Delete" by updating flag
UPDATE iceberg_orders
SET is_deleted = TRUE
WHERE customer_id = 'cust_1001';

-- Queries always filter out deleted rows
SELECT * FROM iceberg_orders
WHERE is_deleted = FALSE;

-- Streaming job continues normally (no conflicts)
-- Hard deletion happens later in batch:
DELETE FROM iceberg_orders WHERE is_deleted = TRUE;
```

### Alternative: Logical vs Physical Deletion

**Logical deletion** (preferred for compliance):
```sql
-- Keep all data, just mark as deleted
UPDATE iceberg_orders SET is_deleted = TRUE WHERE customer_id = 'cust_1001';

-- Queries filter: WHERE is_deleted = FALSE
-- Audit can still see history
-- Complies with retention periods
```

**Physical deletion** (for aggressive cleanup):
```sql
-- Actually remove data
DELETE FROM iceberg_orders WHERE customer_id = 'cust_1001';

-- Irrevocable
-- Use only after legal retention period
```

---

## 5. Compliance Patterns

### Pattern 1: GDPR Compliance with Multi-Region Storage

```
Raw data (EU region):
  s3://eu-data-lake/iceberg_orders/

Pseudonymized copy (US region):
  s3://us-data-lake/iceberg_orders_pseudonym/

Deletion policy:
  1. GDPR request arrives
  2. Delete from EU region
  3. Delete from US region
  4. Log deletion in audit table
```

```sql
-- Replicate and pseudonymize
INSERT INTO iceberg_orders_pseudonym
SELECT 
  id,
  MD5(customer_id) as customer_id,  -- Irreversible hash
  order_ts,
  amount,
  status
FROM iceberg_orders;

-- On deletion request:
DELETE FROM iceberg_orders WHERE customer_id = 'cust_1001';
DELETE FROM iceberg_orders_pseudonym WHERE customer_id = MD5('cust_1001');
```

### Pattern 2: Retention by Data Sensitivity

```sql
-- High sensitivity (PII): Keep 1 year
DELETE FROM iceberg_orders_pii
WHERE order_ts < CURRENT_TIMESTAMP - INTERVAL '1' YEAR;

-- Medium sensitivity (aggregates): Keep 5 years
DELETE FROM iceberg_orders_aggregates
WHERE order_ts < CURRENT_TIMESTAMP - INTERVAL '5' YEAR;

-- Low sensitivity (anonymized): Keep indefinitely
-- (No deletion)
```

### Pattern 3: Audit Trail Immutability

```sql
-- Create a separate immutable audit database
CREATE CATALOG audit_catalog WITH (
  'type' = 'iceberg',
  'warehouse' = 's3://audit-warehouse/',
  'catalog-impl' = 'org.apache.iceberg.aws.glue.GlueCatalog'
);

USE CATALOG audit_catalog;

-- Audit table has stricter policies:
CREATE TABLE IF NOT EXISTS deletion_audit (
  deletion_id STRING,
  customer_id STRING,
  data_deleted TIMESTAMP(3),
  deletion_reason STRING,
  deletion_approved_by STRING,
  deletion_timestamp TIMESTAMP(3)
)
WITH (
  'write.format.default' = 'parquet'
  -- Consider adding encryption:
  -- 'encryption.key-id' = 'audit-key'
);

-- Every deletion creates an immutable audit record
INSERT INTO deletion_audit
VALUES ('del_xyz', 'cust_1001', CURRENT_TIMESTAMP, 'GDPR', 'admin@company.com', CURRENT_TIMESTAMP);
```

---

## Practical Workflow: Complete GDPR Request

### Day 1: Request Received

```
From: data-compliance@company.com
To: data-engineering@company.com

GDPR Deletion Request #REQ-2026-0501
Customer: cust_1001
Requested: 2026-05-01
Deadline: 2026-05-31
```

### Day 2: Validation & Archival

```sql
-- Step 1: Find all customer data
SELECT COUNT(*) FROM iceberg_orders WHERE customer_id = 'cust_1001';
-- Result: 5 orders

-- Step 2: Archive before deletion
INSERT INTO iceberg_orders_gdpr_archive
SELECT * FROM iceberg_orders WHERE customer_id = 'cust_1001';

-- Step 3: Log the request
INSERT INTO deletion_audit (deletion_id, customer_id, data_deleted, deletion_reason)
VALUES ('REQ-2026-0501', 'cust_1001', CURRENT_TIMESTAMP, 'GDPR Article 17');

-- Step 4: Verify archive
SELECT COUNT(*) FROM iceberg_orders_gdpr_archive WHERE customer_id = 'cust_1001';
-- Should match: 5
```

### Day 3: Execute Deletion

```sql
SET 'execution.runtime-mode' = 'batch';

CREATE CATALOG IF NOT EXISTS iceberg_catalog WITH (...);
USE CATALOG iceberg_catalog;
USE `default`;

-- Perform deletion
DELETE FROM iceberg_orders WHERE customer_id = 'cust_1001';

-- Verify deletion
SELECT COUNT(*) FROM iceberg_orders WHERE customer_id = 'cust_1001';
-- Should be: 0
```

### Day 4: Confirmation Report

```sql
-- Generate compliance report
SELECT
  'REQ-2026-0501' as request_id,
  'cust_1001' as customer_id,
  5 as orders_deleted,
  CURRENT_TIMESTAMP as deletion_date,
  'COMPLETED' as status,
  'All PII permanently removed from iceberg_orders. Data archived in iceberg_orders_gdpr_archive for retention.' as notes;
```

---

## Checklist: GDPR Compliance

- [ ] Data inventory complete (know what PII you store)
- [ ] Retention policy documented
- [ ] Deletion process automated
- [ ] Audit log immutable and complete
- [ ] Time travel enabled (Iceberg snapshots)
- [ ] Archive process in place (legal hold)
- [ ] Deletion request workflow (tracking)
- [ ] Soft delete option available (if conflicts)
- [ ] Encryption at rest and in transit
- [ ] Regular retention audits (monthly)

---

## Tools for Compliance

### Check data age distribution
```sql
SELECT 
  EXTRACT(YEAR FROM order_ts) as year,
  EXTRACT(MONTH FROM order_ts) as month,
  COUNT(*) as count
FROM iceberg_orders
GROUP BY year, month
ORDER BY year DESC, month DESC;
```

### Find customers with old data
```sql
SELECT customer_id, MIN(order_ts) as first_order, MAX(order_ts) as last_order
FROM iceberg_orders
GROUP BY customer_id
HAVING MAX(order_ts) < CURRENT_TIMESTAMP - INTERVAL '2' YEAR
ORDER BY last_order;
```

### Track deletion requests
```sql
SELECT 
  request_id,
  customer_id,
  request_date,
  status,
  deleted_row_count,
  CURRENT_TIMESTAMP - request_date as age_of_request
FROM deletion_requests
ORDER BY request_date DESC;
```
