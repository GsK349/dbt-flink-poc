#!/usr/bin/env python3
"""
Generate partitioned test parquet files in Hive-style directory structure.

Usage:
    python3 scripts/generate_partitioned_test_data.py

Creates:
    output/partitioned_orders/
    ├── year=2026/month=04/day=28/orders.parquet
    ├── year=2026/month=04/day=29/orders.parquet
    └── year=2026/month=05/day=01/orders.parquet
"""

import os
import pandas as pd
import pyarrow.parquet as pq
import pyarrow as pa
from datetime import datetime, timedelta
import random

def generate_test_data(start_date, end_date, output_base_dir="output/partitioned_orders"):
    """Generate partitioned parquet files."""

    os.makedirs(output_base_dir, exist_ok=True)

    current_date = start_date
    file_count = 0
    total_rows = 0

    while current_date <= end_date:
        year = current_date.year
        month = current_date.month
        day = current_date.day

        # Generate 50-200 orders for this day
        num_orders = random.randint(50, 200)
        orders = []

        for i in range(num_orders):
            orders.append({
                "id": f"{year}{month:02d}{day:02d}{i:04d}",
                "customer_id": f"cust_{1000 + (i % 100)}",
                "order_ts": (current_date + timedelta(hours=i % 24)).isoformat(),
                "amount": round(10.0 + random.random() * 500, 2),
                "status": random.choice(["created", "processing", "completed", "cancelled"])
            })

        df = pd.DataFrame(orders)
        table = pa.Table.from_pandas(df)

        # Create partition directory structure
        partition_dir = os.path.join(
            output_base_dir,
            f"year={year}",
            f"month={month:02d}",
            f"day={day:02d}"
        )
        os.makedirs(partition_dir, exist_ok=True)

        # Write parquet file
        output_file = os.path.join(partition_dir, "orders.parquet")
        pq.write_table(table, output_file, compression="snappy")

        file_count += 1
        total_rows += len(df)

        print(f"✓ {partition_dir}/orders.parquet ({len(df)} rows)")

        current_date += timedelta(days=1)

    print(f"\n✅ Generated {file_count} files with {total_rows} total rows")
    print(f"📁 Location: {os.path.abspath(output_base_dir)}")
    return output_base_dir

if __name__ == "__main__":
    # Generate data for April 28 - May 1, 2026
    start = datetime(2026, 4, 28)
    end = datetime(2026, 5, 1)

    output_dir = generate_test_data(start, end)

    print("\n📋 Sample query to load this data:")
    print("""
    CREATE TABLE orders_from_s3_partitioned (
      id STRING,
      customer_id STRING,
      order_ts TIMESTAMP(3),
      amount DECIMAL(10,2),
      status STRING,
      year INT,
      month INT,
      day INT
    ) WITH (
      'connector' = 'filesystem',
      'path' = 's3://your-bucket-name/orders/',
      'format' = 'parquet',
      'partition.fields' = 'year,month,day'
    );

    INSERT INTO iceberg_catalog.`default`.iceberg_orders
    SELECT id, customer_id, order_ts, amount, status
    FROM orders_from_s3_partitioned
    WHERE year = 2026 AND month = 4;
    """)

    print("\n📤 Next: Upload to S3")
    print(f"   aws s3 sync {output_dir} s3://your-bucket-name/orders/")
