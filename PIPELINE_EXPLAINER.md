# Flink + dbt + Iceberg Pipeline — Complete Explainer

*For someone who has never used Apache Flink, dbt, or Java libraries before.*

---

## Table of Contents

1. [What Is This Pipeline Doing?](#1-what-is-this-pipeline-doing)
2. [The Big Picture — Data Flow Diagram](#2-the-big-picture)
3. [Technology Glossary — What Each Tool Is](#3-technology-glossary)
4. [The Infrastructure Layer — Docker Compose](#4-the-infrastructure-layer)
5. [The Java JAR Layer — Why So Many .jar Files?](#5-the-java-jar-layer)
6. [Stage 1 — Kafka (Event Ingestion)](#6-stage-1--kafka-event-ingestion)
7. [Stage 2 — Apache Flink (Stream Processing)](#7-stage-2--apache-flink-stream-processing)
8. [Stage 3 — Apache Iceberg on S3 (Storage)](#8-stage-3--apache-iceberg-on-s3-storage)
9. [Stage 4 — dbt (Analytics Transformation)](#9-stage-4--dbt-analytics-transformation)
10. [How the Pieces Connect — Catalogs Explained](#10-how-the-pieces-connect--catalogs-explained)
11. [AWS Services Used](#11-aws-services-used)
12. [How to Run the Full Pipeline (Step by Step)](#12-how-to-run-the-full-pipeline-step-by-step)
13. [Common Errors and Why They Happen](#13-common-errors-and-why-they-happen)
14. [File-by-File Reference](#14-file-by-file-reference)

---

## 1. What Is This Pipeline Doing?

At the highest level, this project answers the question:

> *"A customer just placed an order. How do I get that event from the moment it happened all the way into an analytics table I can query?"*

The answer involves five steps:

```
Order event happens
       ↓
Kafka  (holds the event message temporarily)
       ↓
Flink  (reads from Kafka, processes in real time, writes to storage)
       ↓
Iceberg on S3  (permanent structured storage, like a smart data lake)
       ↓
dbt    (creates analytics-ready views on top of the Iceberg data)
       ↓
Analyst queries the final table
```

This specific project is a **proof-of-concept (POC)**, meaning it's a demonstration of the architecture working, not a production system. The data is 3 sample orders repeated across multiple checkpoint cycles.

---

## 2. The Big Picture

```
┌─────────────────────────────────────────────────────────────────────┐
│  YOUR LAPTOP (Docker)                                               │
│                                                                     │
│  ┌─────────────┐    ┌──────────────────────────────────────────┐   │
│  │  Zookeeper  │───▶│               Apache Kafka               │   │
│  │  (port 2181)│    │           topic: orders_topic            │   │
│  └─────────────┘    └──────────────────────┬─────────────────-─┘   │
│                                            │ reads JSON events      │
│                     ┌──────────────────────▼──────────────────┐    │
│                     │          Apache Flink Cluster           │    │
│                     │                                         │    │
│                     │  JobManager (port 8081 — web UI)        │    │
│                     │  TaskManager (does actual work)         │    │
│                     │  SQL Gateway (port 8083 — REST API)     │    │
│                     │                                         │    │
│                     │  Streaming job: Kafka → Iceberg         │    │
│                     └──────────────────────┬────────────────-─┘    │
│                                            │ writes parquet files   │
└────────────────────────────────────────────┼────────────────────────┘
                                             │
                    ┌────────────────────────▼──────────────────┐
                    │              AWS Cloud                    │
                    │                                           │
                    │  S3 Bucket: flink-iceberg-warehouse       │
                    │  └── default.db/iceberg_orders/           │
                    │       ├── data/  (parquet files)          │
                    │       └── metadata/  (JSON metadata)      │
                    │                                           │
                    │  AWS Glue Data Catalog                    │
                    │  └── database: default                    │
                    │       └── table: iceberg_orders           │
                    └───────────────────┬───────────────────────┘
                                        │
                    ┌───────────────────▼───────────────────────┐
                    │              dbt (runs locally)           │
                    │                                           │
                    │  Connects to Flink SQL Gateway            │
                    │  Creates views on top of Iceberg data:    │
                    │   - stg_orders (staging, raw pass-through)│
                    │   - mart_orders_summary (aggregated)      │
                    └───────────────────────────────────────────┘
```

---

## 3. Technology Glossary

### Apache Kafka
Think of Kafka as a **bulletin board for messages**. When a system (e.g. an order service) wants to say "order 1001 was placed," it posts the message to Kafka's bulletin board. Other systems (like Flink) subscribe to that board and get every message in order. Kafka holds messages for a configurable period so that if a subscriber goes down and comes back up, it can replay all the messages it missed.

- A **topic** is one named bulletin board. This project uses `orders_topic`.
- Messages are called **events** or **records**.
- Kafka is a distributed system that uses **Zookeeper** for coordination (which is why Zookeeper appears in docker-compose).

### Apache Flink
Flink is a **stream processing engine**. Imagine a factory assembly line: events flow in one end (from Kafka), Flink workers do operations on each event (filtering, enriching, joining), and results flow out the other end (to Iceberg). It handles this continuously and in real time.

- A **job** is one pipeline (in this case: read from Kafka, write to Iceberg).
- The **JobManager** is the coordinator — it assigns work, tracks progress, and handles failures.
- The **TaskManager** is the worker — it actually runs the code.
- **Flink SQL** lets you describe the job in SQL instead of Java code.
- The **SQL Gateway** is a REST API server that lets external tools (like dbt) submit SQL to Flink over HTTP.
- **Checkpointing** is Flink's way of taking periodic snapshots of progress so it can recover from failures without reprocessing everything.

### Apache Iceberg
Iceberg is an **open table format for large analytic datasets**. Think of it as a smarter version of storing CSV files in S3. Regular files in S3 have no schema, no versioning, and no transactions. Iceberg adds:

- **Schema**: Every file knows its column names and types.
- **Snapshots**: Every write creates a new snapshot. You can time-travel to any past version.
- **ACID transactions**: Multiple writers won't corrupt data.
- **File format**: Data is stored as **Parquet** (a compressed, columnar binary format — much faster to query than CSV).

When Flink writes to Iceberg, it creates Parquet files in S3 and also writes JSON metadata files that describe the table structure and which files belong to which snapshot.

### dbt (data build tool)
dbt is a **transformation tool for analytics engineers**. It lets you write SQL `SELECT` statements that transform raw data into clean, analytics-ready models. dbt handles:

- Running your SQL in the right order (dependency graph).
- Materializing results as tables or views in your database/engine.
- Testing data quality.
- Generating documentation.

In this project, dbt connects to Flink (not a traditional database like Postgres). The **dbt-flink-adapter** is a plugin that translates dbt's standard SQL calls into Flink SQL Gateway REST API calls.

### AWS Glue Data Catalog
Glue is AWS's **managed metadata store** for data lakes. When Iceberg needs to store metadata about a table (its schema, location, snapshots), it can use Glue as the catalog. This means:

- You can query the Iceberg table from any AWS service (Athena, EMR, etc.) — they all look up table metadata from the same Glue catalog.
- Glue holds table name → S3 location → schema mapping.
- The actual data still lives in S3; Glue only holds the map.

### Parquet
A binary file format for columnar data. Unlike CSV (which stores all columns for row 1, then row 2, etc.), Parquet stores all values for column 1 together, then column 2, etc. This makes analytical queries (e.g. `SUM(amount)`) extremely fast because you only read the one column you need, not entire rows.

---

## 4. The Infrastructure Layer

### File: `docker-compose.yml`

Docker Compose defines five services that all run as containers on your laptop:

```
┌──────────────┐  ┌──────────────┐  ┌──────────────┐  ┌──────────────┐  ┌──────────────┐
│  zookeeper   │  │    kafka     │  │ jobmanager   │  │ taskmanager  │  │ sql-gateway  │
│  port: 2181  │  │  port: 9092  │  │  port: 8081  │  │ (no ext port)│  │  port: 8083  │
└──────────────┘  └──────────────┘  └──────────────┘  └──────────────┘  └──────────────┘
```

#### Why Zookeeper?
Kafka (in older versions) uses Zookeeper to elect leaders, store configuration, and coordinate its broker nodes. Zookeeper is essentially a distributed configuration database that Kafka relies on to function.

#### The `flink-dbt-poc:1.0` image
Three services (jobmanager, taskmanager, sql-gateway) all use the same custom Docker image defined in `Dockerfile.flink`:

```dockerfile
FROM apache/flink:1.20.1-scala_2.12
COPY lib/*.jar /opt/flink/lib/
```

This takes the official Flink image and adds the connector JARs from the `lib/` folder. That's it — two lines. The JARs are what give Flink the ability to talk to Kafka and Iceberg/S3.

#### Environment Variables
Each Flink container receives two critical environment variable blocks:

**`FLINK_PROPERTIES`** — A multi-line string that Flink reads as a config file:
```yaml
FLINK_PROPERTIES: |
  rest.address: jobmanager     # Where the REST API lives (used by SQL Gateway to submit jobs)
  rest.bind-address: 0.0.0.0  # Listen on all network interfaces
  taskmanager.numberOfTaskSlots: 4  # How many parallel tasks this worker can run
```

**`JOB_MANAGER_RPC_ADDRESS: jobmanager`** — Tells the TaskManager and SQL Gateway which hostname to connect to for the JobManager's internal RPC port (6123). Without this, workers try to connect to a stale container hostname and fail.

**`hostname: jobmanager`** — Sets the container's internal DNS name. This must match `JOB_MANAGER_RPC_ADDRESS` exactly. Flink uses Apache Pekko (a distributed actor framework) for internal communication, and Pekko registers itself under the container's hostname. If the hostname doesn't match what workers expect, they connect to the wrong address.

#### AWS Credential passthrough
```yaml
AWS_ACCESS_KEY_ID: ${AWS_ACCESS_KEY_ID}
AWS_SECRET_ACCESS_KEY: ${AWS_SECRET_ACCESS_KEY}
AWS_SESSION_TOKEN: ${AWS_SESSION_TOKEN}
AWS_REGION: ${AWS_REGION:-us-east-1}
```
These are passed from your local environment into every Flink container. The Iceberg AWS bundle reads standard AWS environment variables to authenticate to S3 and Glue — the same way the AWS CLI does.

#### Volumes
```yaml
volumes:
  - ./sql:/opt/flink/sql          # SQL scripts are visible inside containers
  - ./data:/opt/flink/data        # Input data files
  - ./iceberg:/opt/flink/iceberg_warehouse  # Local Iceberg warehouse (not used in AWS mode)
```

#### Startup order and health checks
```yaml
taskmanager:
  depends_on:
    jobmanager:
      condition: service_healthy
```
Docker waits for the JobManager to pass its health check (an HTTP call to `localhost:8081/overview`) before starting the TaskManager. This prevents the TaskManager from starting before the JobManager is ready to accept connections.

---

## 5. The Java JAR Layer

### What is a JAR?
JAR stands for **Java ARchive**. It's a ZIP file containing compiled Java code (.class files) and metadata. When Flink starts, it loads every `.jar` file in `/opt/flink/lib/` into memory. This is how you add functionality to Flink — by dropping JARs into that directory.

### How the JARs Were Obtained
The `pom.xml` file is a **Maven** build descriptor. Maven is Java's package manager (like `pip` for Python or `npm` for Node). The `pom.xml` declares which libraries are needed:

```xml
<dependency>
  <groupId>org.apache.flink</groupId>
  <artifactId>flink-sql-connector-kafka</artifactId>
  <version>3.4.0-1.20</version>
</dependency>
<dependency>
  <groupId>org.apache.iceberg</groupId>
  <artifactId>iceberg-flink-runtime-1.20</artifactId>
  <version>1.10.1</version>
</dependency>
<dependency>
  <groupId>org.apache.iceberg</groupId>
  <artifactId>iceberg-aws-bundle</artifactId>
  <version>1.10.1</version>
</dependency>
```

Running `mvn dependency:copy-dependencies` downloads all these JARs (and their transitive dependencies) into the `lib/` folder. The Dockerfile then copies that folder into the image.

### The Critical JARs Explained

| JAR | What It Does |
|-----|-------------|
| `flink-sql-connector-kafka-3.4.0-1.20.jar` | Teaches Flink SQL the `'connector'='kafka'` syntax. Without it, Flink cannot read from or write to Kafka. |
| `iceberg-flink-runtime-1.20-1.10.1.jar` | Teaches Flink SQL the `'type'='iceberg'` catalog syntax. Provides the IcebergCatalog implementation, the IcebergDynamicTableSink (for writing), and the IcebergDynamicTableSource (for reading). |
| `iceberg-aws-bundle-1.10.1.jar` | Provides the AWS-specific implementations: `GlueCatalog` (talks to AWS Glue API), `S3FileIO` (reads/writes Parquet files to S3). This is a "fat JAR" — it bundles the AWS SDK for Java and all dependencies to avoid classpath conflicts. |
| `hadoop-common-3.3.4.jar` and related | Apache Hadoop's file system abstractions. Iceberg's S3FileIO internally uses Hadoop's FileSystem API. The Hadoop JARs provide the S3A filesystem implementation and configuration utilities. |
| `flink-json-1.20.1.jar` | Teaches Flink the `'format'='json'` connector option for parsing JSON messages from Kafka. |

### Why So Many Jars?
Every JAR in `lib/` is a **transitive dependency** — a library that a library needs. For example:
- `iceberg-aws-bundle` needs `jackson-databind` to parse JSON.
- `hadoop-common` needs `guava` for utility collections.
- `guava` itself is already included by Flink but at a different version, so Hadoop ships a shaded (renamed package) copy to avoid conflicts.

This is a common pain point in the Java ecosystem. The Maven build resolves version conflicts and ensures you have exactly the right set of JARs.

---

## 6. Stage 1 — Kafka (Event Ingestion)

### What Kafka holds in this project
A single topic: `orders_topic`. Each message is a JSON object:

```json
{"id":"1001","customer_id":"cust_1001","order_ts":"2026-04-28 08:00:00","amount":15.50,"status":"created"}
{"id":"1002","customer_id":"cust_1002","order_ts":"2026-04-28 08:05:00","amount":42.00,"status":"completed"}
{"id":"1003","customer_id":"cust_1003","order_ts":"2026-04-28 08:10:00","amount":9.99,"status":"cancelled"}
```

### How to create the topic and publish messages
```bash
# Create the topic (one partition, no replication since it's a single-broker dev setup)
bash scripts/create_kafka_topic.sh

# Publish the 3 sample orders
bash scripts/produce_sample_orders.sh
```

Internally, `produce_sample_orders.sh` pipes JSON lines into `kafka-console-producer` running inside the Kafka container. The producer reads each line from stdin and posts it as a Kafka message.

### Kafka configuration in docker-compose
```yaml
KAFKA_LISTENERS: PLAINTEXT://0.0.0.0:9092          # Listen on all interfaces inside Docker
KAFKA_ADVERTISED_LISTENERS: PLAINTEXT://kafka:9092  # Tell clients to connect via hostname 'kafka'
KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR: 1           # Only 1 broker, so replication = 1
```

`kafka:9092` works as an address because Docker Compose puts all services on the same `flink` network where each service's name is a valid DNS hostname.

---

## 7. Stage 2 — Apache Flink (Stream Processing)

### File: `sql/flink_kafka_to_iceberg.sql`

This is the core of the pipeline. It's a Flink SQL script that defines the streaming job.

#### Step 1 — Set execution mode and checkpointing

```sql
SET 'execution.runtime-mode' = 'streaming';
SET 'execution.checkpointing.interval' = '10s';
SET 'execution.checkpointing.mode' = 'EXACTLY_ONCE';
```

- **streaming mode**: Process events continuously as they arrive (vs. batch mode which processes a fixed dataset and stops).
- **checkpointing every 10s**: Every 10 seconds, Flink takes a snapshot of the job's state and commits any buffered Iceberg data. This is what actually makes data appear in Iceberg — data is written to parquet files at checkpoint time.
- **EXACTLY_ONCE**: Guarantees that even if Flink crashes and replays, each message is written to Iceberg exactly once (not twice). This uses distributed transactions across Kafka consumer offsets and Iceberg commits.

#### Step 2 — Create the Kafka source table

```sql
USE CATALOG default_catalog;
USE default_database;

CREATE TABLE IF NOT EXISTS orders_stream (
  id STRING,
  customer_id STRING,
  order_ts TIMESTAMP(3),
  amount DECIMAL(10,2),
  status STRING
) WITH (
  'connector' = 'kafka',
  'topic' = 'orders_topic',
  'properties.bootstrap.servers' = 'kafka:9092',
  'scan.startup.mode' = 'earliest-offset',
  'format' = 'json',
  'json.timestamp-format.standard' = 'ISO-8601'
);
```

This is **not a real table** — it's a virtual table that tells Flink "when someone reads from `orders_stream`, connect to Kafka topic `orders_topic` and parse each message as JSON with this schema."

Key design choice: this table is created in `default_catalog` (Flink's in-memory catalog), **not** in the Iceberg catalog. This matters because the Iceberg catalog is backed by AWS Glue, and Glue doesn't know how to manage Kafka connector tables.

`scan.startup.mode = 'earliest-offset'` means: start reading from the very beginning of the topic, not just new messages. This ensures historical data is captured.

#### Step 3 — Create the Iceberg catalog and sink table

```sql
CREATE CATALOG iceberg_catalog WITH (
  'type' = 'iceberg',
  'warehouse' = 's3://flink-iceberg-warehouse/',
  'catalog-impl' = 'org.apache.iceberg.aws.glue.GlueCatalog',
  'io-impl' = 'org.apache.iceberg.aws.s3.S3FileIO'
);

CREATE TABLE IF NOT EXISTS iceberg_catalog.`default`.iceberg_orders (
  id STRING,
  customer_id STRING,
  order_ts TIMESTAMP(3),
  amount DECIMAL(10,2),
  status STRING
) WITH (
  'write.format.default' = 'parquet'
);
```

`CREATE CATALOG` registers a new catalog in Flink's session. The properties tell it:
- `type = iceberg`: Use the Iceberg catalog implementation (from `iceberg-flink-runtime` JAR).
- `warehouse = s3://...`: The root S3 path where all table data and metadata will be stored.
- `catalog-impl = GlueCatalog`: Use AWS Glue as the metadata store (stores table schemas, snapshot history).
- `io-impl = S3FileIO`: Use the S3 implementation for reading/writing actual data files.

`CREATE TABLE IF NOT EXISTS` registers the table definition in Glue and creates the initial Iceberg metadata in S3. The backtick-quoted `default` is the Glue database name (backticks are needed because `default` is a reserved word in Flink SQL).

#### Step 4 — The streaming INSERT

```sql
INSERT INTO iceberg_catalog.`default`.iceberg_orders
SELECT id, customer_id, order_ts, amount, status
FROM default_catalog.default_database.orders_stream;
```

This is the actual Flink job. It reads every event from the Kafka source and writes it to Iceberg. This job runs **forever** (until manually cancelled) — every new message published to Kafka will be picked up within the next checkpoint interval (10 seconds).

Fully-qualified names (`catalog.database.table`) are used to avoid ambiguity since two catalogs are registered in the same session.

### How to submit the SQL job

```bash
docker exec flink_dbt_poc-jobmanager-1 \
  /opt/flink/bin/sql-client.sh -f /opt/flink/sql/flink_kafka_to_iceberg.sql
```

This runs the Flink SQL client inside the JobManager container and feeds it the SQL file. The client submits the `INSERT` as a streaming job and exits; the job continues running in the cluster.

### Flink Web UI
Once running, visit `http://localhost:8081` to see:
- Running jobs with their state (RUNNING, FINISHED, FAILED)
- Throughput metrics (records processed per second)
- Task manager status

### Flink SQL Gateway
The SQL Gateway (port 8083) is a separate REST API server that allows submitting SQL over HTTP — without needing to be inside the container. dbt uses this interface. It supports:
- `POST /v1/sessions` — create a new SQL session
- `POST /v1/sessions/{id}/statements` — submit a SQL statement
- `GET /v1/sessions/{id}/operations/{opId}/status` — poll for completion
- `GET /v1/sessions/{id}/operations/{opId}/result/{page}` — fetch result pages

**Important quirk**: The result API is paginated. Page 0 always returns only the column schema with empty data. Actual row data starts on page 1. This caught us when earlier scripts stopped at page 0 and reported 0 rows.

---

## 8. Stage 3 — Apache Iceberg on S3 (Storage)

### What Iceberg stores and where

After the Flink job runs a checkpoint, S3 contains:

```
s3://flink-iceberg-warehouse/
└── default.db/
    └── iceberg_orders/
        ├── data/
        │   ├── 00000-0-31701020....parquet   ← actual order rows
        │   └── 00000-0-b1d7d44c....parquet   ← more rows from next checkpoint
        └── metadata/
            ├── v1.metadata.json              ← initial table definition
            ├── v2.metadata.json              ← after first write
            ├── ...
            ├── snap-xxxx.avro               ← snapshot manifest files
            └── version-hint.text             ← pointer to latest metadata version
```

### What is a snapshot?
Every successful Flink checkpoint triggers an Iceberg **commit**, which creates a new snapshot. A snapshot is a consistent point-in-time view of the table: "as of this moment, the table contains exactly these data files." This is how Iceberg provides ACID guarantees — a reader always sees a complete, consistent snapshot, even while a writer is adding more files.

### What AWS Glue stores
Glue stores the table's logical metadata:
- Table name: `iceberg_orders`
- Database: `default`
- Location: `s3://flink-iceberg-warehouse/default.db/iceberg_orders/`
- SerDe info (how to read the files): points to the Iceberg InputFormat
- Schema: column names and types

Any tool that understands Iceberg + Glue (Flink, Spark, Trino, AWS Athena) can read this table just by knowing the Glue database and table name, without needing to know the S3 path.

### Why Parquet?
Parquet stores data in a compressed, columnar binary format. For a query like `SELECT SUM(amount) FROM iceberg_orders`, Parquet lets the query engine read only the `amount` column bytes without touching `id`, `customer_id`, etc. On datasets with many columns, this can be 10-100x faster than row-oriented formats like JSON or CSV.

---

## 9. Stage 4 — dbt (Analytics Transformation)

### Project structure

```
flink_dbt_poc/
├── dbt_project.yml          ← project config (name, profile, model paths)
├── dbt/
│   ├── profiles.yml         ← connection config (host, port, database)
│   ├── macros/
│   │   └── setup_iceberg_catalog.sql  ← runs before any model
│   └── models/
│       ├── sources.yml      ← (intentionally minimal — see note below)
│       ├── stg_orders.sql   ← staging model
│       └── mart_orders_summary.sql  ← mart model
```

### File: `dbt_project.yml`

```yaml
name: flink_dbt_poc
version: '1.0'
config-version: 2
profile: flink_iceberg       # which connection in profiles.yml to use
model-paths: ["dbt/models"]
macro-paths: ["dbt/macros"]

on-run-start:
  - "{{ setup_iceberg_catalog() }}"  # run this macro before any models
```

`on-run-start` hooks run at the very beginning of `dbt run`. The `setup_iceberg_catalog()` macro registers the Iceberg catalog in the SQL Gateway session before any models try to reference `iceberg_catalog.*` tables.

### File: `dbt/profiles.yml`

```yaml
flink_iceberg:
  target: dev
  outputs:
    dev:
      type: flink          # use the dbt-flink-adapter plugin
      host: localhost
      port: 8083           # SQL Gateway REST port
      session_name: dbt_session
      database: default_catalog   # Flink catalog to use as default
      schema: default_database    # Flink database within that catalog
      threads: 1
```

This tells dbt to connect to the Flink SQL Gateway at `localhost:8083`. The adapter creates a session, caches the session handle in `~/.dbt/flink-session.yml` to reuse across runs, and submits each model's SQL as a statement.

### File: `dbt/macros/setup_iceberg_catalog.sql`

```sql
{% macro setup_iceberg_catalog() %}
{% if execute %}
{% set sql %}
CREATE CATALOG IF NOT EXISTS iceberg_catalog WITH (
  'type' = 'iceberg',
  'warehouse' = 's3://flink-iceberg-warehouse/',
  'catalog-impl' = 'org.apache.iceberg.aws.glue.GlueCatalog',
  'io-impl' = 'org.apache.iceberg.aws.s3.S3FileIO'
)
{% endset %}
{% set result = run_query(sql) %}
{{ log("Iceberg catalog registered", info=True) }}
{% endif %}
{% endmacro %}
```

This is a Jinja2 macro (dbt's templating language). `{% if execute %}` means "only run this during an actual `dbt run`, not during parsing/compilation." `run_query()` submits the SQL to the Flink SQL Gateway and waits for it to finish. Without this, models that reference `iceberg_catalog.*` would fail because Flink sessions start with only `default_catalog` registered.

### File: `dbt/models/stg_orders.sql`

```sql
{{ config(materialized='view') }}

select
  id,
  customer_id,
  order_ts,
  amount,
  status
from iceberg_catalog.`default`.iceberg_orders
```

This is the **staging model** — a thin pass-through that reads directly from the Iceberg table. `materialized='view'` means dbt runs `CREATE VIEW stg_orders AS (SELECT ...)`. A view doesn't store data; it's a saved query that runs each time you query it.

`{{ config(...) }}` is Jinja2 syntax — the dbt adapter reads this configuration block before compiling the SQL.

### File: `dbt/models/mart_orders_summary.sql`

```sql
{{ config(materialized='view') }}

select
  customer_id,
  FLOOR(order_ts TO DAY) as order_date,
  count(*) as order_count,
  sum(amount) as total_amount
from {{ ref('stg_orders') }}
group by customer_id, FLOOR(order_ts TO DAY)
```

This is the **mart model** — the analytics-ready aggregation. It answers: "For each customer, on each day, how many orders did they place and what was the total amount?"

Two important Flink SQL specifics here:
- `{{ ref('stg_orders') }}` — dbt's cross-model reference. dbt replaces this with the actual view name and ensures `stg_orders` is created before this model runs.
- `FLOOR(order_ts TO DAY)` — Flink SQL's way of truncating a timestamp to the start of the day. Standard SQL `DATE_TRUNC('day', order_ts)` does not work in Flink SQL.

### How dbt connects to Flink

```
dbt run
  │
  ├── Reads dbt_project.yml and profiles.yml
  ├── Compiles Jinja2 templates into pure SQL
  ├── Creates a new SQL Gateway session (or reuses cached one)
  │
  ├── on-run-start: POST /v1/sessions/{id}/statements
  │   └── CREATE CATALOG IF NOT EXISTS iceberg_catalog ...
  │
  ├── Model 1 (stg_orders):
  │   └── POST /v1/sessions/{id}/statements
  │       └── DROP VIEW IF EXISTS stg_orders;
  │           CREATE VIEW stg_orders AS (SELECT ...);
  │
  └── Model 2 (mart_orders_summary):
      └── POST /v1/sessions/{id}/statements
          └── DROP VIEW IF EXISTS mart_orders_summary;
              CREATE VIEW mart_orders_summary AS (SELECT ...);
```

**Session caching**: After the first run, dbt saves the session handle to `~/.dbt/flink-session.yml`. On subsequent runs it reuses the same session. If you restart the Docker stack, the old session no longer exists, and you must delete this file: `rm ~/.dbt/flink-session.yml`.

### Running dbt

```bash
.venv39/bin/dbt run --profiles-dir dbt
```

Output on success:
```
14:46:20  Running with dbt=1.3.7
14:46:20  [INFO] Iceberg catalog registered
14:46:28  1 of 2 OK created sql view model stg_orders ............. [FINISHED in 8.44s]
14:46:29  2 of 2 OK created sql view model mart_orders_summary .... [FINISHED in 0.31s]
14:46:29  Done. PASS=2 WARN=0 ERROR=0 SKIP=0 TOTAL=2
```

---

## 10. How the Pieces Connect — Catalogs Explained

This is one of the trickier concepts. Flink can have multiple **catalogs** registered at the same time, each backed by a different storage system:

```
Flink SQL Session
│
├── default_catalog  (built-in, in-memory, disappears when session ends)
│   └── default_database
│       └── orders_stream  ← Kafka connector table (virtual)
│
└── iceberg_catalog  (registered via CREATE CATALOG, backed by AWS Glue)
    └── default  (Glue database)
        ├── iceberg_orders  ← real Iceberg table (persists in S3)
        ├── orders_stream   ← (old leftover from earlier experiments)
        └── raw_orders      ← (old leftover from earlier experiments)
```

### Why the Kafka table must be in `default_catalog`
The Kafka connector creates a virtual table definition in Flink's memory. It doesn't write anything to Glue. If you accidentally create it inside `iceberg_catalog`, Glue stores the table metadata but Glue doesn't understand the `connector=kafka` property — it just stores it as an Iceberg table with no data. When Flink tries to read from it, Iceberg's reader is used instead of Kafka's reader, and you get 0 records.

### Why the Iceberg table must be in `iceberg_catalog`
Iceberg needs its catalog (`GlueCatalog`) to track table snapshots, schema evolution, and file manifests. The `default_catalog` has no concept of snapshots — it's just a name registry. Data written to a table in `default_catalog` with `connector=filesystem` goes to a local path and has none of Iceberg's ACID guarantees.

---

## 11. AWS Services Used

### Amazon S3
Object storage. Holds:
- Parquet data files: `s3://flink-iceberg-warehouse/default.db/iceberg_orders/data/*.parquet`
- Iceberg metadata JSON: `s3://flink-iceberg-warehouse/default.db/iceberg_orders/metadata/*.json`

S3 is accessed by the `S3FileIO` class (from `iceberg-aws-bundle`), which uses the AWS SDK for Java and reads credentials from environment variables.

### AWS Glue Data Catalog
Serverless metadata store. Holds the table definition (schema + S3 location). Used by:
- Flink (via `GlueCatalog`) when creating tables or querying table metadata
- Any other AWS service that wants to query the same table (Athena, EMR, etc.)

### IAM Credentials
The pipeline needs IAM permissions for:
- `s3:GetObject`, `s3:PutObject`, `s3:DeleteObject` on `flink-iceberg-warehouse`
- `glue:CreateTable`, `glue:GetTable`, `glue:UpdateTable` on the Glue database

These are provided via `AWS_ACCESS_KEY_ID` + `AWS_SECRET_ACCESS_KEY` + `AWS_SESSION_TOKEN` environment variables.

---

## 12. How to Run the Full Pipeline (Step by Step)

### Prerequisites
- Docker Desktop running
- AWS credentials configured (with S3 + Glue permissions)
- Python 3.9+ with the project virtualenv

### Step 1 — Set AWS credentials

Edit `.env.aws` with your credentials:
```bash
AWS_REGION=us-east-1
AWS_ACCESS_KEY_ID=your_key_id
AWS_SECRET_ACCESS_KEY=your_secret_key
AWS_SESSION_TOKEN=your_session_token_if_using_sts
```

Export them to your shell:
```bash
export $(cat .env.aws | grep -v '#' | xargs)
```

### Step 2 — Start the Docker stack

```bash
docker-compose up -d --build
```

Wait ~30 seconds for the JobManager health check to pass. Verify:
```bash
docker ps
# Should show 5 containers: zookeeper, kafka, jobmanager, taskmanager, sql-gateway
```

Visit http://localhost:8081 to confirm Flink Web UI is up.

### Step 3 — Create the Kafka topic and publish test data

```bash
bash scripts/create_kafka_topic.sh
bash scripts/produce_sample_orders.sh
```

### Step 4 — Submit the streaming job

```bash
docker exec flink_dbt_poc-jobmanager-1 \
  /opt/flink/bin/sql-client.sh -f /opt/flink/sql/flink_kafka_to_iceberg.sql
```

Wait ~15 seconds for the first checkpoint to complete. The job will show as RUNNING in the Flink Web UI. Data appears in S3 after the first successful checkpoint.

### Step 5 — Run dbt

```bash
rm -f ~/.dbt/flink-session.yml   # clear any stale session from previous run
.venv39/bin/dbt run --profiles-dir dbt
```

Both models should pass: `PASS=2 WARN=0 ERROR=0`.

### Step 6 — Query the results

Use the SQL Gateway via Python:

```python
import json, urllib.request, time

BASE = "http://localhost:8083/v1"

def post(url, data):
    req = urllib.request.Request(url, json.dumps(data).encode(), {"Content-Type": "application/json"})
    with urllib.request.urlopen(req) as r:
        return json.loads(r.read())

def get(url):
    with urllib.request.urlopen(url) as r:
        return json.loads(r.read())

# Create session
SID = post(f"{BASE}/sessions", {"sessionName": "query"})["sessionHandle"]

# Register catalog
op = post(f"{BASE}/sessions/{SID}/statements", {
    "statement": "CREATE CATALOG IF NOT EXISTS iceberg_catalog WITH ('type'='iceberg','warehouse'='s3://flink-iceberg-warehouse/','catalog-impl'='org.apache.iceberg.aws.glue.GlueCatalog','io-impl'='org.apache.iceberg.aws.s3.S3FileIO')"
})["operationHandle"]
# wait for FINISHED...

# Query mart
op = post(f"{BASE}/sessions/{SID}/statements", {
    "statement": "SELECT * FROM mart_orders_summary",
    "executionConfig": {"execution.runtime-mode": "batch"}
})["operationHandle"]
# wait for FINISHED, then fetch page 1 (not page 0!) for data
```

Expected result:
```
customer_id   order_date           order_count   total_amount
cust_1001     2026-04-28T00:00:00  5             77.50
cust_1002     2026-04-28T00:00:00  5             210.00
cust_1003     2026-04-28T00:00:00  5             49.95
```

(5 records per customer because the 3 messages were sent multiple times across checkpoints.)

---

## 13. Common Errors and Why They Happen

### "TaskManager could not connect to JobManager"
**Symptom**: TaskManager logs show `pekko.tcp://flink@<some-hash>:6123` connection refused.

**Cause**: Flink uses Apache Pekko (an actor framework) for internal RPC. Pekko registers the JobManager actor under the container's hostname. If the hostname is a random Docker container hash (like `d4db04dbdfd7`) but the TaskManager is told to connect to `jobmanager`, they don't match.

**Fix**: Set `hostname: jobmanager` on the jobmanager service in docker-compose.yml AND set `JOB_MANAGER_RPC_ADDRESS: jobmanager` on the TaskManager.

### "SQL Gateway can't submit job — connection refused to 0.0.0.0:8081"
**Symptom**: Jobs submitted via SQL Gateway fail immediately; Flink REST API shows no new jobs.

**Cause**: `rest.address: 0.0.0.0` tells the SQL Gateway to connect to `0.0.0.0:8081` when submitting jobs, but `0.0.0.0` is not a valid target address — it means "all interfaces" when listening, not when connecting.

**Fix**: Set `rest.address: jobmanager` (the actual hostname) in FLINK_PROPERTIES.

### "Kafka source reads 0 records"
**Symptom**: Flink job runs but no data appears in Iceberg.

**Cause**: The `orders_stream` table was accidentally created in `iceberg_catalog` during a previous test. `CREATE TABLE IF NOT EXISTS` saw the existing (empty) Iceberg version and used it, so Flink used the Iceberg reader instead of the Kafka connector.

**Fix**: Always create Kafka source tables in `default_catalog`. Use `USE CATALOG default_catalog` before the Kafka `CREATE TABLE`.

### "dbt sources hook drops the Iceberg table"
**Symptom**: After `dbt run`, the Iceberg table is empty and recreated with no columns.

**Cause**: The `dbt-flink-adapter` has a built-in `create_sources()` hook that runs `DROP TABLE IF EXISTS` and `CREATE TABLE ... WITH ()` for every source defined in `sources.yml`. If sources.yml lists `iceberg_orders` without column definitions, it drops the real Iceberg table and creates a broken empty one.

**Fix**: Keep `sources.yml` empty (just `version: 2`) and reference the Iceberg table directly by full path in model SQL.

### "SELECT returns 0 rows via REST API"
**Symptom**: A SELECT query via SQL Gateway REST API finishes successfully but appears to return no data.

**Cause**: The Flink SQL Gateway REST result API is paginated. Page 0 always contains only the column schema (empty data array). Row data starts on page 1.

**Fix**: Always start reading from page 0 (to get the schema), then follow the `nextResultUri` to page 1, 2, etc. until `resultType == "EOS"`.

### "Session '...' does not exist" in dbt
**Symptom**: `dbt run` fails with a session-not-found error.

**Cause**: dbt caches the SQL Gateway session handle in `~/.dbt/flink-session.yml`. If the Docker stack was restarted, all sessions are gone, but the old handle is still cached.

**Fix**: `rm ~/.dbt/flink-session.yml` before running dbt after any stack restart.

### "DATE_TRUNC is not found"
**Symptom**: dbt run fails on `mart_orders_summary` with "No match found for function signature date_trunc".

**Cause**: `DATE_TRUNC` is standard SQL but Flink SQL doesn't support it. Flink uses a different syntax.

**Fix**: Replace `date_trunc('day', order_ts)` with `FLOOR(order_ts TO DAY)`.

---

## 14. File-by-File Reference

| File | Purpose |
|------|---------|
| `docker-compose.yml` | Defines and connects all 5 Docker services |
| `Dockerfile.flink` | Builds the custom Flink image with connector JARs |
| `pom.xml` | Maven descriptor listing all Java library dependencies |
| `lib/*.jar` | Downloaded JARs — Flink connectors, Iceberg, Hadoop, AWS SDK |
| `sql/flink_kafka_to_iceberg.sql` | Flink SQL script: creates Kafka source, Iceberg sink, streaming INSERT job |
| `scripts/create_kafka_topic.sh` | Creates the `orders_topic` Kafka topic |
| `scripts/produce_sample_orders.sh` | Publishes 3 sample JSON order events to Kafka |
| `scripts/run_flink_sql.sh` | Helper to run a SQL file via the embedded SQL client |
| `data/batch/sample_orders.json` | Sample data file (3 orders in JSON format) |
| `dbt_project.yml` | dbt project config: name, profile, model/macro paths, on-run-start hook |
| `dbt/profiles.yml` | dbt connection config: Flink SQL Gateway at localhost:8083 |
| `dbt/macros/setup_iceberg_catalog.sql` | on-run-start macro: registers iceberg_catalog in the SQL Gateway session |
| `dbt/models/sources.yml` | dbt sources definition (intentionally empty to prevent adapter's auto-drop behavior) |
| `dbt/models/stg_orders.sql` | dbt staging model: view over iceberg_catalog.default.iceberg_orders |
| `dbt/models/mart_orders_summary.sql` | dbt mart model: orders aggregated by customer and day |
| `.env.aws` | Local AWS credentials (not committed to git) |
| `requirements.txt` | Python packages: dbt-core==1.3.7, dbt-flink-adapter==1.3.11 |

---

## Summary

This pipeline demonstrates a complete **modern data stack** on a laptop:

1. **Kafka** buffers real-time order events reliably.
2. **Flink** reads those events continuously and writes them to Iceberg using EXACTLY_ONCE semantics.
3. **Iceberg** stores the data as Parquet files in S3, with AWS Glue managing the table metadata — making the data accessible to the entire AWS ecosystem.
4. **dbt** sits on top of Flink's SQL layer, creating analytics-ready views that any BI tool can query.

The entire stack is containerized (except S3/Glue which are AWS-managed), reproducible on any machine with Docker and valid AWS credentials, and can be extended to handle real production event volumes by scaling out Flink TaskManagers and Kafka partitions.
