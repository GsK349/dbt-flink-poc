# Batch Processing with S3 Parquet Files — Complete Guide

This guide explains how to switch from simulated batch data to real S3 parquet files, including handling partitioned data.

---

## Table of Contents

1. [Batch vs. Streaming — What's the Difference?](#1-batch-vs-streaming)
2. [Reading Unpartitioned Parquet from S3](#2-reading-unpartitioned-parquet)
3. [Reading Partitioned Parquet from S3](#3-reading-partitioned-parquet)
4. [Setting Up Test Data in S3](#4-setting-up-test-data-in-s3)
5. [Running Batch Jobs](#5-running-batch-jobs)
6. [Performance Optimization](#6-performance-optimization)
7. [Common Issues](#7-common-issues)

---

## 1. Batch vs. Streaming

### Current Setup (Streaming)
```
Kafka topic (continuous events)
    ↓
Flink streaming job (runs forever)
    ↓
Checkpoints every 10s (data written to Iceberg)
    ↓
Iceberg table (accumulates all events)
```

**Characteristics:**
- Events arrive continuously
- Job runs 24/7
- Low latency (seconds to minutes)
- Data is processed as it arrives

### Batch Processing (New)
```
S3 parquet files (fixed dataset)
    ↓
Flink batch job (reads all files, processes, stops)
    ↓
One final commit (all data written to Iceberg at once)
    ↓
Iceberg table
```

**Characteristics:**
- Fixed set of files to process
- Job starts, processes, and exits
- Higher throughput (can use all resources at once)
- Good for historical data loads, migrations, or periodic bulk imports

### When to Use Which?

| Scenario | Use |
|----------|-----|
| Order events arriving in real time | Streaming |
| Daily batch of CSV files from partner | Batch |
| Historical data migration | Batch |
| Real-time analytics dashboard | Streaming |
| End-of-day reconciliation report | Batch |

---

## 2. Reading Unpartitioned Parquet

### File Structure in S3
```
s3://my-data-lake/
├── orders/
│   ├── orders_2026_04_28.parquet
│   ├── orders_2026_04_29.parquet
│   └── orders_2026_04_30.parquet
```

### Flink SQL
```sql
SET 'execution.runtime-mode' = 'batch';

CREATE TABLE orders_from_s3 (
  id STRING,
  customer_id STRING,
  order_ts TIMESTAMP(3),
  amount DECIMAL(10,2),
  status STRING
) WITH (
  'connector' = 'filesystem',
  'path' = 's3://my-data-lake/orders/',
  'format' = 'parquet'
);

INSERT INTO iceberg_catalog.`default`.iceberg_orders
SELECT * FROM orders_from_s3;
```

### What Happens
1. Flink scans the S3 path: `s3://my-data-lake/orders/`
2. Finds all `.parquet` files in that directory
3. Reads all files in parallel (using multiple TaskManagers)
4. Inserts rows into the Iceberg table
5. Job finishes and exits

### Key Points
- The path can point to a **directory** (Flink scans all `.parquet` files inside)
- Or a **single file**: `'path' = 's3://bucket/orders_final.parquet'`
- Flink uses the S3A filesystem (from Hadoop) which is built into the Iceberg AWS bundle

---

## 3. Reading Partitioned Parquet

### What are Partitions?

**Hive-style partitioning** divides data by values, improving query performance:

```
s3://my-data-lake/orders/
├── year=2026/month=04/day=28/
│   ├── 00000-0-xxx.parquet
│   └── 00000-0-yyy.parquet
├── year=2026/month=04/day=29/
│   └── 00000-0-zzz.parquet
└── year=2026/month=05/day=01/
    └── 00000-0-www.parquet
```

The partition columns (`year`, `month`, `day`) are derived from the folder path, not stored in the data files. This allows:
- **Partition pruning**: Query only `year=2026/month=05/` and skip April data
- **Faster queries**: Metadata tells you which files contain which dates
- **Organized data**: Partitions group related data together

### Why Partition?

With 10 years of order history:
- **Unpartitioned**: Read all 3.6M parquet files, parse every one → slow
- **Partitioned**: Read only the folders you need → fast

### Flink SQL for Partitioned Data

```sql
SET 'execution.runtime-mode' = 'batch';

CREATE TABLE orders_partitioned (
  id STRING,
  customer_id STRING,
  order_ts TIMESTAMP(3),
  amount DECIMAL(10,2),
  status STRING,
  -- Partition columns MUST be listed after regular columns
  year INT,
  month INT,
  day INT
) WITH (
  'connector' = 'filesystem',
  'path' = 's3://my-data-lake/orders/',
  'format' = 'parquet',
  'partition.fields' = 'year,month,day'
);

-- Load specific partitions only
INSERT INTO iceberg_catalog.`default`.iceberg_orders
SELECT id, customer_id, order_ts, amount, status
FROM orders_partitioned
WHERE year = 2026 AND month = 5;  -- Only May 2026
```

### How It Works

1. **Registration**: `'partition.fields' = 'year,month,day'` tells Flink these are partition columns
2. **Discovery**: Flink scans the S3 path and discovers all partitions (year=2026/month=04/day=28, etc.)
3. **Partition pruning**: The `WHERE year = 2026 AND month = 5` clause filters out directories that don't match
4. **Reading**: Only files in matching directories are read and decompressed

### Partition Key Design

**Best practice**: Use columns that are naturally hierarchical and commonly filtered:

```
Good:
├── year=2026/month=05/day=01/    (temporal hierarchy, daily snapshots)
├── country=US/state=CA/          (geographic hierarchy)
└── product_category=Electronics/ (categorical)

Avoid:
├── order_id=1001/  (too many partitions, no benefit)
└── amount=15.50/   (many decimal values, poor grouping)
```

---

## 4. Setting Up Test Data in S3

### Create Test Parquet Files Locally

```bash
# Install pyarrow for parquet writing
pip install pyarrow pandas

# Create sample data
python3 << 'EOF'
import pandas as pd
import pyarrow.parquet as pq
import pyarrow as pa
from datetime import datetime, timedelta
import os

# Create sample orders
orders = [
    {"id": f"1{i:03d}", "customer_id": f"cust_{1000+i}", 
     "order_ts": (datetime(2026, 4, 28) + timedelta(hours=i)).isoformat(),
     "amount": 10.0 + (i % 100), "status": ["created", "completed", "cancelled"][i % 3]}
    for i in range(100)
]

df = pd.DataFrame(orders)

# Convert to parquet
table = pa.Table.from_pandas(df)
os.makedirs("output/test_parquet", exist_ok=True)
pq.write_table(table, "output/test_parquet/orders_batch_001.parquet")

print("Created: output/test_parquet/orders_batch_001.parquet")
print(f"Rows: {len(df)}")
print("\nFirst 5 rows:")
print(df.head())
EOF
```

### Upload to S3

```bash
# Unpartitioned
aws s3 cp output/test_parquet/orders_batch_001.parquet \
  s3://my-data-lake/orders/orders_batch_001.parquet

# Partitioned (with year/month/day structure)
aws s3 cp output/test_parquet/orders_batch_001.parquet \
  s3://my-data-lake/orders/year=2026/month=04/day=28/orders_batch_001.parquet
```

### Create Multiple Partitions

```bash
python3 << 'EOF'
import pandas as pd
import pyarrow.parquet as pq
import pyarrow as pa
from datetime import datetime, timedelta
import os

# Create data for multiple days
for day in range(28, 31):  # April 28-30
    orders = [
        {
            "id": f"{day}{i:04d}",
            "customer_id": f"cust_{1000 + (i % 50)}",
            "order_ts": (datetime(2026, 4, day) + timedelta(hours=i)).isoformat(),
            "amount": 10.0 + (i % 100),
            "status": ["created", "completed", "cancelled"][i % 3]
        }
        for i in range(50)
    ]
    
    df = pd.DataFrame(orders)
    table = pa.Table.from_pandas(df)
    
    path = f"output/orders/year=2026/month=04/day={day:02d}"
    os.makedirs(path, exist_ok=True)
    pq.write_table(table, f"{path}/orders.parquet")
    print(f"Created: {path}/orders.parquet ({len(df)} rows)")
EOF

# Upload entire directory structure to S3
aws s3 sync output/orders/ s3://my-data-lake/orders/
```

---

## 5. Running Batch Jobs

### Step 1: Edit SQL File

Replace `s3://your-bucket-name/path/...` with your actual S3 paths in:
- `sql/batch_s3_parquet.sql` (unpartitioned)
- `sql/batch_s3_partitioned_parquet.sql` (partitioned)

### Step 2: Submit Job

```bash
# Unpartitioned
docker exec flink_dbt_poc-jobmanager-1 \
  /opt/flink/bin/sql-client.sh -f /opt/flink/sql/batch_s3_parquet.sql

# Partitioned
docker exec flink_dbt_poc-jobmanager-1 \
  /opt/flink/bin/sql-client.sh -f /opt/flink/sql/batch_s3_partitioned_parquet.sql
```

### Step 3: Monitor

Visit http://localhost:8081 to see:
- Job progress (% complete)
- Throughput (records/sec)
- Task manager CPU/memory usage

### Step 4: Query Results

```bash
.venv39/bin/dbt run --profiles-dir dbt
```

This creates views on top of the newly loaded Iceberg table.

---

## 6. Performance Optimization

### 1. Parallel Reading

Flink automatically reads parquet files in parallel across TaskManagers. More TaskManager task slots = faster reading.

```yaml
# docker-compose.yml
taskmanager:
  environment:
    FLINK_PROPERTIES: |
      taskmanager.numberOfTaskSlots: 8  # Increase from 4 to 8
```

### 2. Partition Pruning

Always filter partitions in the `WHERE` clause:

```sql
-- ❌ SLOW: Scans all partitions
SELECT * FROM orders_partitioned;

-- ✅ FAST: Only reads matching partitions
SELECT * FROM orders_partitioned WHERE year = 2026 AND month = 5;
```

### 3. Parquet Compression

Use Snappy (default, good balance) or Gzip (better compression, slower):

```python
pq.write_table(table, "file.parquet", compression="snappy")   # Fast
pq.write_table(table, "file.parquet", compression="gzip")     # Compact
```

### 4. Column Pruning

dbt automatically does this. Only SELECT the columns you need:

```sql
-- ❌ SLOWER: Reads all columns
SELECT * FROM orders_partitioned;

-- ✅ FASTER: Only reads needed columns
SELECT id, customer_id, amount FROM orders_partitioned;
```

Parquet's columnar format skips unneeded columns entirely — major speed gain.

### 5. Batch Size

For very large files (10GB+), consider splitting into multiple files:

```
orders/year=2026/month=05/day=01/
├── orders_part_1.parquet  (1GB)
├── orders_part_2.parquet  (1GB)
└── orders_part_3.parquet  (1GB)
```

Flink reads them in parallel within the same partition.

---

## 7. Common Issues

### Issue 1: "Could not find parquet file"

**Symptom**: Job fails with "No files matched the path pattern"

**Cause**: Wrong S3 path or empty directory

**Fix**:
```bash
# Verify files exist
aws s3 ls s3://my-data-lake/orders/ --recursive

# Check credentials are passed correctly
docker exec flink_dbt_poc-jobmanager-1 env | grep AWS
```

### Issue 2: "Parquet file is corrupted"

**Symptom**: Job fails parsing a specific parquet file

**Cause**: File was truncated during upload or not written properly

**Fix**:
```bash
# Verify file integrity locally
python3 -c "import pyarrow.parquet as pq; pq.read_table('file.parquet').num_rows"

# Re-upload if corrupted
aws s3 cp file.parquet s3://bucket/path/file.parquet
```

### Issue 3: "Partition columns not found"

**Symptom**: Error "Column ... not found"

**Cause**: Partition column definition doesn't match folder structure

**Fix**: Ensure folder structure matches partition column names:

```
✅ Correct:
year=2026/month=05/day=01/
→ partition.fields = 'year,month,day'

❌ Wrong:
y=2026/m=05/d=01/
→ Won't match 'year,month,day'
```

### Issue 4: Partition pruning not working

**Symptom**: WHERE clause doesn't filter directories, job reads all data

**Cause**: Partition column types don't match

**Fix**:
```sql
-- ❌ Wrong: Column type is INT but query uses STRING
WHERE year = '2026'

-- ✅ Correct
WHERE year = 2026
```

---

## Comparison: Batch vs. Streaming in This Project

| Aspect | Streaming (Current) | Batch (New) |
|--------|-------------------|-----------|
| **Data source** | Kafka topic | S3 parquet files |
| **Execution mode** | `streaming` | `batch` |
| **Job lifecycle** | Runs forever | Runs once, exits |
| **Checkpointing** | Every 10s | One commit at end |
| **Use case** | Real-time events | Historical data, bulk loads |
| **SQL files** | `flink_kafka_to_iceberg.sql` | `batch_s3_parquet.sql` or `batch_s3_partitioned_parquet.sql` |

---

## Next Steps

1. **Generate test parquet files** locally (see section 4)
2. **Upload to S3** (create unpartitioned first, then partitioned)
3. **Update SQL paths** in the batch SQL files
4. **Submit a batch job** and monitor progress
5. **Compare performance**: Batch vs. streaming throughput

Both jobs can coexist — you can have a streaming Kafka→Iceberg job running 24/7 **and** periodically run batch jobs to backfill historical data into the same Iceberg table.
