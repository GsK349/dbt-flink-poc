-- Flink SQL: UPDATE and DELETE examples on Iceberg
-- Use batch mode for modifications

SET 'execution.runtime-mode' = 'batch';

-- Register Iceberg catalog
CREATE CATALOG IF NOT EXISTS iceberg_catalog WITH (
  'type' = 'iceberg',
  'warehouse' = '${ICEBERG_WAREHOUSE_PATH}',
  'catalog-impl' = 'org.apache.iceberg.aws.glue.GlueCatalog',
  'io-impl' = 'org.apache.iceberg.aws.s3.S3FileIO'
);

USE CATALOG iceberg_catalog;
USE `default`;

-- ============================================================================
-- EXAMPLE 1: Check current data
-- ============================================================================

SELECT id, customer_id, status, amount FROM iceberg_orders LIMIT 10;

SELECT COUNT(*) as total_rows FROM iceberg_orders;

-- Find duplicates
SELECT id, COUNT(*) as count FROM iceberg_orders
GROUP BY id
HAVING COUNT(*) > 1;


-- ============================================================================
-- EXAMPLE 2: Single row UPDATE - fix one order status
-- ============================================================================

UPDATE iceberg_orders
SET status = 'completed'
WHERE id = '1001' AND customer_id = 'cust_1001';

-- Verify the update
SELECT id, customer_id, status FROM iceberg_orders WHERE id = '1001';


-- ============================================================================
-- EXAMPLE 3: Bulk UPDATE - mark all April orders as verified
-- ============================================================================

UPDATE iceberg_orders
SET status = 'verified'
WHERE order_ts >= TIMESTAMP '2026-04-01 00:00:00'
  AND order_ts < TIMESTAMP '2026-05-01 00:00:00';

-- Verify
SELECT COUNT(*) as verified_count FROM iceberg_orders
WHERE status = 'verified';


-- ============================================================================
-- EXAMPLE 4: UPDATE multiple columns
-- ============================================================================

UPDATE iceberg_orders
SET
  status = 'cancelled',
  amount = 0.00
WHERE id = '1003' AND customer_id = 'cust_1003';


-- ============================================================================
-- EXAMPLE 5: UPDATE with calculation
-- ============================================================================

-- Apply 10% discount to all orders over $100
UPDATE iceberg_orders
SET amount = amount * 0.9
WHERE amount > 100.00;

-- Verify discounts were applied
SELECT id, amount FROM iceberg_orders WHERE amount <= 90.00;


-- ============================================================================
-- EXAMPLE 6: Single row DELETE - remove one problematic order
-- ============================================================================

DELETE FROM iceberg_orders
WHERE id = '1002' AND customer_id = 'cust_1002';

-- Verify deletion
SELECT COUNT(*) as remaining FROM iceberg_orders;


-- ============================================================================
-- EXAMPLE 7: Bulk DELETE - remove all cancelled orders from April
-- ============================================================================

DELETE FROM iceberg_orders
WHERE status = 'cancelled'
  AND order_ts >= TIMESTAMP '2026-04-01 00:00:00'
  AND order_ts < TIMESTAMP '2026-05-01 00:00:00';

-- Count remaining orders
SELECT status, COUNT(*) FROM iceberg_orders GROUP BY status;


-- ============================================================================
-- EXAMPLE 8: DELETE based on complex condition
-- ============================================================================

-- Delete test/dummy orders (customer_id starts with 'test_')
DELETE FROM iceberg_orders
WHERE customer_id LIKE 'test_%';

-- Or delete orders with negative amounts (data quality issue)
DELETE FROM iceberg_orders
WHERE amount < 0;


-- ============================================================================
-- EXAMPLE 9: Find and fix duplicates
-- ============================================================================

-- First, see the duplicates
SELECT id, customer_id, order_ts, COUNT(*) as count
FROM iceberg_orders
GROUP BY id, customer_id, order_ts
HAVING COUNT(*) > 1;

-- Option A: Delete one copy (keeping the first occurrence)
-- This requires knowing the exact duplicate to delete
DELETE FROM iceberg_orders
WHERE id = '1001'
  AND customer_id = 'cust_1001'
  AND order_ts = '2026-04-28T08:00:00';


-- ============================================================================
-- EXAMPLE 10: MERGE (Upsert) - Update if exists, Insert if new
-- Note: MERGE may not be supported in all Flink versions
-- ============================================================================

-- Uncomment to test (requires Flink 1.18+):
/*
MERGE INTO iceberg_orders AS t
USING (
  SELECT '1002' as id, 'cust_1002' as customer_id,
         TIMESTAMP '2026-04-28 08:05:00' as order_ts,
         50.00 as amount, 'completed' as status
) AS s
ON t.id = s.id AND t.customer_id = s.customer_id
WHEN MATCHED THEN
  UPDATE SET
    status = s.status,
    amount = s.amount,
    order_ts = s.order_ts
WHEN NOT MATCHED THEN
  INSERT (id, customer_id, order_ts, amount, status)
  VALUES (s.id, s.customer_id, s.order_ts, s.amount, s.status);
*/


-- ============================================================================
-- EXAMPLE 11: Verify changes with snapshot comparison
-- ============================================================================

-- Current snapshot
SELECT COUNT(*) as current_row_count FROM iceberg_orders;

-- List all snapshots (shows history of changes)
-- This is a Flink SQL extension:
-- SELECT * FROM iceberg_orders.snapshots;  -- May not work in all Flink versions


-- ============================================================================
-- EXAMPLE 12: Data quality correction pattern
-- ============================================================================

-- Fix multiple data quality issues in one UPDATE
UPDATE iceberg_orders
SET
  status = CASE
    WHEN status IS NULL THEN 'unknown'
    WHEN status = 'CREATED' THEN 'created'  -- normalize case
    WHEN status = 'COMPLETED' THEN 'completed'
    ELSE status
  END,
  amount = CASE
    WHEN amount < 0 THEN 0  -- negative amounts are errors
    WHEN amount > 100000 THEN 99999.99  -- cap extreme values
    ELSE amount
  END
WHERE status IS NULL
   OR status NOT IN ('created', 'processing', 'completed', 'cancelled')
   OR amount < 0
   OR amount > 100000;

-- Verify fixes
SELECT COUNT(*) as fixed_rows FROM iceberg_orders
WHERE amount >= 0 AND amount <= 100000;


-- ============================================================================
-- EXAMPLE 13: Conditional DELETE - remove based on multiple criteria
-- ============================================================================

-- Delete old test/staging data
DELETE FROM iceberg_orders
WHERE (
  customer_id LIKE 'test_%'
  OR customer_id LIKE 'staging_%'
  OR customer_id LIKE 'demo_%'
)
AND order_ts < TIMESTAMP '2026-04-01 00:00:00';

-- Or delete incomplete orders older than 30 days
DELETE FROM iceberg_orders
WHERE status IN ('created', 'processing')
  AND order_ts < TIMESTAMP '2026-03-31 00:00:00';


-- ============================================================================
-- FINAL VERIFICATION
-- ============================================================================

-- Show summary statistics after all modifications
SELECT
  COUNT(*) as total_rows,
  COUNT(DISTINCT customer_id) as unique_customers,
  COUNT(DISTINCT status) as unique_statuses,
  MIN(amount) as min_amount,
  MAX(amount) as max_amount,
  AVG(amount) as avg_amount
FROM iceberg_orders;

-- Show distribution by status
SELECT status, COUNT(*) as count FROM iceberg_orders GROUP BY status ORDER BY status;
