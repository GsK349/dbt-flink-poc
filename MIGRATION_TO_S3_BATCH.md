# Migrating from Simulated Batch Data to S3 Parquet Files

This document explains the current setup and how to transition to real S3 batch processing.

---

## Current Setup (Simulated)

### How Data Currently Flows

```
data/batch/sample_orders.json (3 static orders)
    ↓
Kafka producer script (manually published)
    ↓
Kafka topic: orders_topic (holds 3 events)
    ↓
Flink streaming job (runs forever)
    ↓
Kafka → Iceberg checkpoint every 10s
    ↓
Iceberg table (same 3 orders repeated across 5 checkpoints = 15 rows)
```

### Files Involved

| File | Purpose |
|------|---------|
| `data/batch/sample_orders.json` | 3 hand-written order records |
| `scripts/produce_sample_orders.sh` | Manually publishes those 3 to Kafka |
| `sql/flink_kafka_to_iceberg.sql` | Streaming job (uses Kafka) |

### Current Limitations

❌ Only 3 sample orders  
❌ Manual data publishing  
❌ Can't test with real-world data volume  
❌ Can't test partitioned data scenarios  

---

## New Setup (S3 Batch)

### How Data Will Flow

**Option 1: Unpartitioned Files**
```
S3 bucket: flink-data-lake/orders/
├── orders_2026_04_28.parquet (1000 rows)
├── orders_2026_04_29.parquet (1500 rows)
└── orders_2026_04_30.parquet (2000 rows)
    ↓
Flink batch job (reads all 3 files)
    ↓
One-time INSERT into Iceberg
    ↓
Iceberg table (4500 total rows)
```

**Option 2: Partitioned Files**
```
S3 bucket: flink-data-lake/orders/
├── year=2026/month=04/day=28/orders.parquet (1000 rows)
├── year=2026/month=04/day=29/orders.parquet (1500 rows)
└── year=2026/month=04/day=30/orders.parquet (2000 rows)
    ↓
Flink batch job (discovers partitions, optionally filters)
    ↓
One-time INSERT into Iceberg (or incremental for specific dates)
    ↓
Iceberg table (4500 rows, organized by date)
```

### Advantages

✅ Real S3 data (no manual publishing)  
✅ Test with large datasets  
✅ Learn partition pruning optimization  
✅ Batch and streaming can coexist  
✅ Closer to production architecture  

---

## Step-by-Step Migration

### Phase 1: Generate Test Data (Local)

```bash
# Install dependencies
pip install pandas pyarrow

# Generate partitioned test data
python3 scripts/generate_partitioned_test_data.py
```

This creates:
```
output/partitioned_orders/
├── year=2026/month=04/day=28/orders.parquet (50-200 rows)
├── year=2026/month=04/day=29/orders.parquet (50-200 rows)
├── year=2026/month=04/day=30/orders.parquet (50-200 rows)
└── year=2026/month=05/day=01/orders.parquet (50-200 rows)
```

### Phase 2: Upload to S3

```bash
# Set your S3 bucket and AWS credentials
export AWS_REGION=us-east-1
export S3_BUCKET=your-data-lake-bucket

# Upload partitioned data
aws s3 sync output/partitioned_orders/ \
  s3://$S3_BUCKET/orders/

# Verify upload
aws s3 ls s3://$S3_BUCKET/orders/ --recursive
```

Expected output:
```
2026-05-01 10:30:15       1234 orders/year=2026/month=04/day=28/orders.parquet
2026-05-01 10:30:16       1567 orders/year=2026/month=04/day=29/orders.parquet
2026-05-01 10:30:17       1890 orders/year=2026/month=04/day=30/orders.parquet
2026-05-01 10:30:18       2145 orders/year=2026/month=05/day=01/orders.parquet
```

### Phase 3: Update SQL File

Edit `sql/batch_s3_partitioned_parquet.sql`:

```sql
-- Change this:
'path' = 's3://your-bucket-name/path/to/partitioned/orders/'

-- To your actual path:
'path' = 's3://your-data-lake-bucket/orders/'
```

### Phase 4: Run Batch Job

```bash
# Keep the streaming job running (optional)
# Or stop it: click "Cancel" on http://localhost:8081

# Run the batch job
docker exec flink_dbt_poc-jobmanager-1 \
  /opt/flink/bin/sql-client.sh -f /opt/flink/sql/batch_s3_partitioned_parquet.sql
```

Monitor progress at http://localhost:8081

### Phase 5: Query Results with dbt

```bash
rm -f ~/.dbt/flink-session.yml
.venv39/bin/dbt run --profiles-dir dbt
```

Run analytics models on the newly loaded data.

---

## Comparison: Before vs. After

| Aspect | Before (Simulated) | After (S3 Batch) |
|--------|-------------------|-----------------|
| **Data source** | Hand-written JSON in repo | Real parquet files in S3 |
| **Data volume** | 3 orders | Scalable (100s-millions) |
| **Partitioning** | Not applicable | Hive-style (year/month/day) |
| **Setup** | Manual `produce_sample_orders.sh` | Automated script + S3 upload |
| **Job type** | Streaming (forever) | Batch (one-time) |
| **Restart behavior** | Replays Kafka (checkpointing) | Reruns full INSERT |
| **Production readiness** | POC only | Closer to real pipeline |

---

## Hybrid Approach: Batch + Streaming

You don't have to choose. Many production systems use both:

```
Historical data (2025-2026-03)
    ↓
Batch job: Load from S3 parquet into Iceberg
    ↓
Iceberg table (contains history)
    ↓
Streaming job: Kafka → Iceberg (adds today's events continuously)
    ↓
Real-time analytics on current + historical data
```

### Implementation

```bash
# Week 1: Load historical data via batch
docker exec flink_dbt_poc-jobmanager-1 \
  /opt/flink/bin/sql-client.sh -f /opt/flink/sql/batch_s3_parquet.sql

# Wait for completion, then:

# Week 2: Start streaming job for new events
docker exec flink_dbt_poc-jobmanager-1 \
  /opt/flink/bin/sql-client.sh -f /opt/flink/sql/flink_kafka_to_iceberg.sql

# Both jobs now write to the same Iceberg table
# Queries see: historical + real-time data
```

Both use `iceberg_catalog.default.iceberg_orders_batch` (or same table name) — Iceberg automatically merges snapshots from different writers.

---

## Performance Characteristics

### Unpartitioned Data
```
1000 files × 1GB each = 1TB total

Read time (4 task slots):
  - With S3A parallelism: ~5-10 minutes
  - Throughput: 100-200 MB/s from S3

Without partition pruning:
  - Scans all 1000 files regardless of WHERE clause
```

### Partitioned Data (Same 1TB)
```
Same 1000 files, but organized as:
  year=2025/month=01/day=01/...
  year=2025/month=01/day=02/...
  ...
  year=2026/month=12/day=31/...

Read time with partition pruning:
  - WHERE year = 2026 AND month = 5: Only scans ~30 files
  - Time: ~1-2 minutes (15x faster!)
  - Throughput: Still 100-200 MB/s, but less total data
```

**Lesson**: Partitioning becomes critical at scale.

---

## Troubleshooting

### "No files matched the path pattern"

**Check 1**: Verify files exist in S3
```bash
aws s3 ls s3://bucket/orders/ --recursive
```

**Check 2**: Verify Flink can reach S3
```bash
# Inside jobmanager container
docker exec flink_dbt_poc-jobmanager-1 bash -c \
  "aws s3 ls s3://bucket/orders/ --recursive"
```

**Check 3**: Check AWS credentials
```bash
docker exec flink_dbt_poc-jobmanager-1 env | grep AWS_
```

### "Parquet file corruption"

Verify files locally:
```bash
# Download and check
aws s3 cp s3://bucket/orders/year=2026/month=04/day=28/orders.parquet .
python3 -c "import pyarrow.parquet as pq; t = pq.read_table('orders.parquet'); print(f'Rows: {t.num_rows}')"
```

### "Job hangs or is very slow"

**Check**: Is Flink reading sequentially or in parallel?
```
# Visit http://localhost:8081/jobs/JOBID
# Look at: Task Parallelism

If showing "1 task" → Increase task slots in docker-compose.yml
If showing "8 tasks" → Check S3 bandwidth limits (use CloudWatch)
```

---

## Files Reference

| File | Purpose | Status |
|------|---------|--------|
| `sql/batch_s3_parquet.sql` | Batch read from unpartitioned S3 files | New |
| `sql/batch_s3_partitioned_parquet.sql` | Batch read from partitioned S3 files | New |
| `scripts/generate_partitioned_test_data.py` | Generate local test parquet files | New |
| `sql/flink_kafka_to_iceberg.sql` | Stream from Kafka | Existing |
| `data/batch/sample_orders.json` | Simulated data | Still exists, no longer needed |

---

## Next Steps

1. ✅ Generate test data: `python3 scripts/generate_partitioned_test_data.py`
2. ✅ Upload to S3: `aws s3 sync output/partitioned_orders/ s3://bucket/orders/`
3. ✅ Update SQL path in `batch_s3_partitioned_parquet.sql`
4. ✅ Run batch job: `docker exec ... sql-client.sh -f batch_s3_partitioned_parquet.sql`
5. ✅ Query with dbt: `.venv39/bin/dbt run --profiles-dir dbt`

Estimated time: 10-15 minutes (mostly S3 upload time).
