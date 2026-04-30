# Using Kafka Headers for CDC Operations — Best Practice

This guide explains how to use Kafka message headers to define operations (INSERT, UPDATE, DELETE) instead of embedding them in the message body.

---

## Table of Contents

1. [Why Kafka Headers?](#1-why-kafka-headers)
2. [How Flink Reads Headers](#2-how-flink-reads-headers)
3. [Implementation](#3-implementation)
4. [Producer Examples](#4-producer-examples)
5. [Comparison: Headers vs. Payload](#5-comparison-headers-vs-payload)

---

## 1. Why Kafka Headers?

### The Problem with Embedding Operations in Payload

```json
{
  "id": "1001",
  "customer_id": "cust_1001",
  "amount": 15.50,
  "status": "created",
  "op": "INSERT"  ← Pollutes data payload
}
```

**Issues:**
- ❌ Metadata mixed with data
- ❌ Data schema includes operation (complicates schema evolution)
- ❌ Harder to separate concerns (data vs. operations)
- ❌ Breaks data isolation (operation leaked into records)

### The Solution: Kafka Headers

```
Message:
  Headers: { "operation": "INSERT", "source": "order-service", "timestamp": "2026-04-28T08:00:00" }
  Body:    { "id": "1001", "customer_id": "cust_1001", "amount": 15.50, "status": "created" }
```

**Benefits:**
- ✅ Clean separation: metadata in headers, data in body
- ✅ Schema never includes operation
- ✅ Headers are optional metadata (consumers can ignore them)
- ✅ Standard Kafka pattern (used by Debezium, Confluent, etc.)
- ✅ Consumer flexibility (different consumers use different headers)

---

## 2. How Flink Reads Headers

### Kafka Source with Headers Support

Flink's Kafka connector can expose headers as virtual columns:

```sql
CREATE TABLE orders_with_headers (
  -- Data columns
  id STRING,
  customer_id STRING,
  order_ts TIMESTAMP(3),
  amount DECIMAL(10,2),
  status STRING,
  
  -- Header columns (read-only, virtual)
  headers MAP<STRING, BYTES> METADATA FROM 'value.headers'  -- All headers as map
  -- OR individual headers:
  -- operation STRING METADATA FROM 'value.headers.operation'
  -- source STRING METADATA FROM 'value.headers.source'
  -- timestamp STRING METADATA FROM 'value.headers.timestamp'
) WITH (
  'connector' = 'kafka',
  'topic' = 'orders_topic',
  'properties.bootstrap.servers' = 'kafka:9092',
  'format' = 'json'
);
```

### Key Points

- `METADATA FROM 'value.headers'` — Read **all** headers as a MAP
- `METADATA FROM 'value.headers.operation'` — Read **specific** header by name
- Headers are **bytes**, decode as needed (usually UTF-8 string)
- Headers are **optional** per message (can be null)

---

## 3. Implementation

### Step 1: Define Table with Headers

Create `sql/streaming_cdc_with_headers.sql`:

```sql
SET 'execution.runtime-mode' = 'streaming';
SET 'execution.checkpointing.interval' = '10s';
SET 'execution.checkpointing.mode' = 'EXACTLY_ONCE';

USE CATALOG default_catalog;
USE default_database;

-- Kafka source that exposes headers
CREATE TABLE IF NOT EXISTS orders_cdc_headers (
  id STRING,
  customer_id STRING,
  order_ts TIMESTAMP(3),
  amount DECIMAL(10,2),
  status STRING,
  
  -- Read headers as individual columns
  operation STRING METADATA FROM 'value.headers.operation',  -- INSERT, UPDATE, DELETE
  source STRING METADATA FROM 'value.headers.source',        -- order-service, admin-api
  event_timestamp STRING METADATA FROM 'value.headers.timestamp'
) WITH (
  'connector' = 'kafka',
  'topic' = 'orders_topic',
  'properties.bootstrap.servers' = 'kafka:9092',
  'scan.startup.mode' = 'earliest-offset',
  'format' = 'json',
  'json.timestamp-format.standard' = 'ISO-8601'
);

-- Or read all headers as a map:
CREATE TABLE IF NOT EXISTS orders_cdc_headers_map (
  id STRING,
  customer_id STRING,
  order_ts TIMESTAMP(3),
  amount DECIMAL(10,2),
  status STRING,
  
  -- All headers in one map
  headers MAP<STRING, BYTES> METADATA FROM 'value.headers'
) WITH (
  'connector' = 'kafka',
  'topic' = 'orders_topic',
  'properties.bootstrap.servers' = 'kafka:9092',
  'scan.startup.mode' = 'earliest-offset',
  'format' = 'json'
);

-- Set up Iceberg
CREATE CATALOG IF NOT EXISTS iceberg_catalog WITH (
  'type' = 'iceberg',
  'warehouse' = 's3://flink-iceberg-warehouse/',
  'catalog-impl' = 'org.apache.iceberg.aws.glue.GlueCatalog',
  'io-impl' = 'org.apache.iceberg.aws.s3.S3FileIO'
);

-- Raw CDC log (captures all events with their operations)
CREATE TABLE IF NOT EXISTS iceberg_catalog.`default`.iceberg_orders_cdc_log (
  id STRING,
  customer_id STRING,
  order_ts TIMESTAMP(3),
  amount DECIMAL(10,2),
  status STRING,
  operation STRING,                          -- From header
  source STRING,                             -- From header
  event_timestamp STRING,                    -- From header
  ingested_at TIMESTAMP(3) AS CURRENT_TIMESTAMP
) WITH (
  'write.format.default' = 'parquet'
);

-- Stream: Extract operation from header, store in CDC log
INSERT INTO iceberg_catalog.`default`.iceberg_orders_cdc_log
SELECT
  id,
  customer_id,
  order_ts,
  amount,
  status,
  operation,                                 -- From header
  source,                                    -- From header
  event_timestamp,                           -- From header
  CURRENT_TIMESTAMP
FROM default_catalog.default_database.orders_cdc_headers
WHERE operation IS NOT NULL;                 -- Filter out messages without operation header
```

### Step 2: dbt Materializes Final State

Create `dbt/models/mart_orders_from_headers.sql`:

```sql
{{ config(materialized='view') }}

-- Deduplicates the CDC log and applies operations
SELECT
  id,
  customer_id,
  order_ts,
  amount,
  status,
  latest_operation,
  latest_source,
  latest_event_timestamp
FROM (
  SELECT
    id,
    customer_id,
    order_ts,
    amount,
    status,
    operation as latest_operation,
    source as latest_source,
    event_timestamp as latest_event_timestamp,
    ROW_NUMBER() OVER (PARTITION BY id ORDER BY event_timestamp DESC) as rn
  FROM iceberg_catalog.`default`.iceberg_orders_cdc_log
  WHERE operation != 'DELETE'  -- Exclude deleted
)
WHERE rn = 1  -- Keep only latest
```

---

## 4. Producer Examples

### Java Producer (with Headers)

```java
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.header.internals.RecordHeaders;

KafkaProducer<String, String> producer = new KafkaProducer<>(props);

// Create record with headers
ProducerRecord<String, String> record = new ProducerRecord<>(
  "orders_topic",
  null,  // key
  "{\"id\":\"1001\",\"customer_id\":\"cust_1001\",\"amount\":15.50,\"status\":\"created\"}"
);

// Add headers
record.headers()
  .add("operation", "INSERT".getBytes())
  .add("source", "order-service".getBytes())
  .add("timestamp", "2026-04-28T08:00:00Z".getBytes());

producer.send(record);

// Update: same id, different operation
ProducerRecord<String, String> update = new ProducerRecord<>(
  "orders_topic",
  null,
  "{\"id\":\"1001\",\"customer_id\":\"cust_1001\",\"amount\":15.50,\"status\":\"completed\"}"
);

update.headers()
  .add("operation", "UPDATE".getBytes())
  .add("source", "order-service".getBytes())
  .add("timestamp", "2026-04-28T10:30:00Z".getBytes());

producer.send(update);
```

### Python Producer (with Headers)

```python
from kafka import KafkaProducer
import json

producer = KafkaProducer(bootstrap_servers=['kafka:9092'])

# INSERT event
headers = [
    ('operation', b'INSERT'),
    ('source', b'order-service'),
    ('timestamp', b'2026-04-28T08:00:00Z')
]

producer.send(
  'orders_topic',
  value=json.dumps({
    "id": "1001",
    "customer_id": "cust_1001",
    "amount": 15.50,
    "status": "created"
  }).encode(),
  headers=headers
)

# UPDATE event (same id, different operation)
headers = [
    ('operation', b'UPDATE'),
    ('source', b'order-service'),
    ('timestamp', b'2026-04-28T10:30:00Z')
]

producer.send(
  'orders_topic',
  value=json.dumps({
    "id": "1001",
    "customer_id": "cust_1001",
    "amount": 15.50,
    "status": "completed"
  }).encode(),
  headers=headers
)

# DELETE event
headers = [
    ('operation', b'DELETE'),
    ('source', b'order-service'),
    ('timestamp', b'2026-04-28T12:00:00Z')
]

producer.send(
  'orders_topic',
  value=json.dumps({
    "id": "1001",
    "customer_id": "cust_1001",
    "amount": 15.50,
    "status": "cancelled"
  }).encode(),
  headers=headers
)

producer.flush()
```

### CLI Test (kcat/kafkacat)

```bash
# Produce with headers using kcat
echo '{"id":"1001","customer_id":"cust_1001","amount":15.50,"status":"created"}' | \
  kcat -P -b kafka:9092 -t orders_topic \
    -H "operation=INSERT" \
    -H "source=order-service" \
    -H "timestamp=2026-04-28T08:00:00Z"

# Update
echo '{"id":"1001","customer_id":"cust_1001","amount":15.50,"status":"completed"}' | \
  kcat -P -b kafka:9092 -t orders_topic \
    -H "operation=UPDATE" \
    -H "source=order-service"

# Delete
echo '{"id":"1001"}' | \
  kcat -P -b kafka:9092 -t orders_topic \
    -H "operation=DELETE"

# Consume and display headers
kcat -C -b kafka:9092 -t orders_topic -J | jq '.headers'
```

### Node.js Producer (with Headers)

```javascript
const { Kafka } = require('kafkajs');

const kafka = new Kafka({
  clientId: 'order-producer',
  brokers: ['kafka:9092']
});

const producer = kafka.producer();
await producer.connect();

// INSERT
await producer.send({
  topic: 'orders_topic',
  messages: [
    {
      value: JSON.stringify({
        id: '1001',
        customer_id: 'cust_1001',
        amount: 15.50,
        status: 'created'
      }),
      headers: {
        'operation': 'INSERT',
        'source': 'order-service',
        'timestamp': '2026-04-28T08:00:00Z'
      }
    }
  ]
});

// UPDATE
await producer.send({
  topic: 'orders_topic',
  messages: [
    {
      value: JSON.stringify({
        id: '1001',
        customer_id: 'cust_1001',
        amount: 15.50,
        status: 'completed'
      }),
      headers: {
        'operation': 'UPDATE',
        'source': 'order-service',
        'timestamp': '2026-04-28T10:30:00Z'
      }
    }
  ]
});

await producer.disconnect();
```

---

## 5. Comparison: Headers vs. Payload

### Payload-Based Operation

```json
Message Body:
{
  "id": "1001",
  "customer_id": "cust_1001",
  "amount": 15.50,
  "status": "created",
  "op": "INSERT"     ← Pollutes schema
}

Schema includes: id, customer_id, amount, status, op
```

### Header-Based Operation

```
Kafka Message:
  Headers: { "operation": "INSERT" }
  Body:    { "id": "1001", "customer_id": "cust_1001", "amount": 15.50, "status": "created" }

Schema includes: id, customer_id, amount, status (clean!)
```

### Comparison Table

| Aspect | Headers | Payload |
|--------|---------|---------|
| **Schema Pollution** | ❌ No | ✅ Yes |
| **Consumer Flexibility** | ✅ Can ignore | ❌ Must handle |
| **Schema Evolution** | ✅ Easy | ❌ Breaking change |
| **Data Quality** | ✅ Operation separate | ❌ Mixed concerns |
| **Industry Standard** | ✅ Yes (Debezium) | ❌ Custom |
| **Query Simplicity** | ✅ Metadata separate | ❌ Filter out op field |
| **Storage Efficiency** | ✅ Headers not stored in table | ❌ Stored with data |

---

## Complete Example: Headers-Based CDC

### Step 1: Producer Sends Events with Headers

```python
# events_producer.py

from kafka import KafkaProducer
import json
from datetime import datetime

producer = KafkaProducer(bootstrap_servers=['kafka:9092'])

events = [
    # Order created
    {
        'data': {'id': '1001', 'customer_id': 'cust_1001', 'amount': 15.50, 'status': 'created'},
        'operation': 'INSERT',
        'timestamp': '2026-04-28T08:00:00Z'
    },
    # Order status changes
    {
        'data': {'id': '1001', 'customer_id': 'cust_1001', 'amount': 15.50, 'status': 'processing'},
        'operation': 'UPDATE',
        'timestamp': '2026-04-28T08:15:00Z'
    },
    {
        'data': {'id': '1001', 'customer_id': 'cust_1001', 'amount': 15.50, 'status': 'completed'},
        'operation': 'UPDATE',
        'timestamp': '2026-04-28T10:30:00Z'
    },
    # Order cancelled (DELETE)
    {
        'data': {'id': '1002', 'customer_id': 'cust_1002', 'amount': 42.00, 'status': 'cancelled'},
        'operation': 'DELETE',
        'timestamp': '2026-04-28T12:00:00Z'
    }
]

for event in events:
    producer.send(
        'orders_topic',
        value=json.dumps(event['data']).encode(),
        headers=[
            ('operation', event['operation'].encode()),
            ('timestamp', event['timestamp'].encode()),
            ('source', b'order-service')
        ]
    )
    print(f"Sent {event['operation']}: {event['data']['id']}")

producer.flush()
producer.close()
```

### Step 2: Flink Captures Events with Headers

```sql
-- SQL from streaming_cdc_with_headers.sql
-- Reads headers and stores in CDC log
```

### Step 3: Inspect Results

```bash
# Raw CDC log (all events)
SELECT operation, id, status, event_timestamp 
FROM iceberg_orders_cdc_log 
ORDER BY event_timestamp;

# Output:
# operation | id    | status      | event_timestamp
# ----------|-------|-------------|-------------------
# INSERT    | 1001  | created     | 2026-04-28T08:00:00
# UPDATE    | 1001  | processing  | 2026-04-28T08:15:00
# UPDATE    | 1001  | completed   | 2026-04-28T10:30:00
# DELETE    | 1002  | cancelled   | 2026-04-28T12:00:00
```

```bash
# Current state (dbt view, latest only, excludes deletes)
SELECT id, customer_id, status, latest_operation 
FROM mart_orders_from_headers;

# Output:
# id   | customer_id | status    | latest_operation
# -----|-------------|-----------|------------------
# 1001 | cust_1001   | completed | UPDATE
# (1002 is not shown because operation=DELETE)
```

---

## Headers vs. Other Patterns

| Pattern | Headers | Op Field | Soft Delete | Upsert |
|---------|---------|----------|-------------|--------|
| **Schema Clean** | ✅ | ❌ | ❌ | ✅ |
| **Audit Trail** | ✅ | ✅ | ✅ | ❌ |
| **Deletes** | ✅ | ✅ | ⚠️ (marked) | ❌ |
| **Flexibility** | ✅ | ⚠️ | ⚠️ | ❌ |
| **Industry Standard** | ✅ | ❌ | ❌ | ❌ |

---

## Best Practices

1. **Always use headers for metadata** (operation, source, timestamp)
   - Don't pollute the data payload

2. **Standard header names** (from Debezium convention):
   - `operation` → INSERT, UPDATE, DELETE
   - `source` → Service/system that created the event
   - `timestamp` → Event creation time
   - `version` → Schema version

3. **Encode as UTF-8** in producer:
   ```python
   headers=[('operation', b'INSERT')]  # bytes
   ```

4. **Handle nullable headers** in Flink:
   ```sql
   WHERE operation IS NOT NULL
   ```

5. **Document header contract** for producers:
   ```
   Required headers:
     - operation: INSERT | UPDATE | DELETE
     - timestamp: ISO-8601 timestamp
   
   Optional headers:
     - source: Originating service
     - version: Schema version
   ```

---

## Advantages Summary

✅ **Clean separation of concerns** (metadata vs. data)  
✅ **Doesn't pollute schema** (no extra columns in data)  
✅ **Flexible consumers** (can use or ignore headers)  
✅ **Industry standard** (used by Debezium, Kafka Connect)  
✅ **Audit trail** (operation captured for every event)  
✅ **Supports deletes** (can mark as DELETE in header)  
✅ **Better storage** (headers not stored in tables)

This is the **recommended approach** for production CDC pipelines.
