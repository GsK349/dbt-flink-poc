# Flink + DBT + Iceberg POC

## Current project status

This workspace currently contains:
- `docker-compose.yml` — Flink cluster plus Kafka and Zookeeper
- `pom.xml` — Maven POM that currently downloads Kafka connector jars
- `requirements.txt` — DBT dependency pins for dbt-core and dbt-flink-adaptor
- `lib/` — local connector jars for Flink
- `data/` — batch/stream I/O data directories
- `iceberg/` — mounted Iceberg warehouse path
- `output/` — output mount for Flink
- `ss/` — screenshot attachments

What is missing for the POC:
- actual Flink job code or SQL scripts
- Iceberg connector jars in the Flink classpath
- DBT project connections to the Flink SQL Gateway
- a working DBT `~/.dbt/profiles.yml` configuration
- sample Kafka producer data for the streaming source

## Current pipeline summary

This repo currently runs a non-Iceberg Flink pipeline that unifies batch and streaming data using Flink SQL.

- Batch source: `data/batch/sample_orders.json`
- Streaming source: Kafka topic `orders_topic`
- Flink SQL script: `sql/flink_batch_stream_no_iceberg.sql`
- Output sink: filesystem JSON under `output/orders_no_iceberg`
- DBT runtime: local Python 3.9 virtual environment in `.venv39`
- DBT profile: `dbt/profiles.yml`

### Detailed flow

1. Start the infrastructure with `docker-compose up -d`.
2. Create Kafka topic: `./scripts/create_kafka_topic.sh`.
3. Publish sample events to Kafka: `./scripts/produce_sample_orders.sh`.
4. Submit the Flink SQL job: `./scripts/run_flink_sql.sh`.
5. The SQL script creates three tables:
   - `raw_orders` filesystem batch source
   - `orders_stream` Kafka JSON streaming source
   - `output_orders` filesystem JSON sink
6. The job inserts `raw_orders UNION ALL orders_stream` into `output_orders`.
7. The sink writes combined results into `output/orders_no_iceberg`.
8. DBT connects to the Flink SQL Gateway at `http://localhost:8083` via `dbt debug --profiles-dir dbt`.

### Current status

- Flink engine: running on Docker via `jobmanager`, `taskmanager`, and `sql-gateway`
- Kafka: running and accepting sample JSON orders
- DBT: validated connection to Flink SQL Gateway
- Iceberg: deferred for now; the current working pipeline uses filesystem sinks

## Recommended POC architecture

Goal: integrate streaming + batch in Flink, write to Iceberg, and use DBT for downstream modeling.

1. Ingest sources
   - streaming: Kafka topic or socket source
   - batch: CSV/Parquet file from local filesystem
2. Use Flink Table API / SQL to unify both sources
   - normalize schema
   - union streaming and batch data
3. Write result to Iceberg
   - use Flink Iceberg sink
   - mount Iceberg warehouse on local filesystem or S3
4. Use DBT for batch modeling
   - DBT reads from Iceberg table(s)
   - build derived tables and aggregates

## Suggested workspace layout

```
flink_dbt_poc/
├── docker-compose.yml
├── pom.xml
├── README.md
├── lib/
├── data/
│   └── batch/sample_orders.csv
├── output/
├── sql/
│   └── flink_stream_batch_to_iceberg.sql
└── dbt/
    ├── dbt_project.yml
    ├── profiles.yml.example
    └── models/
        ├── sources.yml
        ├── stg_orders.sql
        └── mart_orders_summary.sql
```

## Quick start

1. Install DBT packages:
   - `pip install -r requirements.txt`
2. Copy the sample profile to your DBT config:
   - `cp dbt/profiles.yml.example ~/.dbt/profiles.yml`
3. Start the infrastructure:
   - `docker-compose up -d`
4. Create the Kafka topic:
   - `./scripts/create_kafka_topic.sh`
5. Publish sample events:
   - `./scripts/produce_sample_orders.sh`
6. Run the Flink SQL job:
   - `./scripts/run_flink_sql.sh`
7. Confirm Flink SQL Gateway is available at `http://localhost:8083`.

## Next step for a dummy streaming + batch POC

1. Add a batch file in `data/batch/`.
2. Create a Flink SQL job that defines:
   - a filesystem batch source
   - a Kafka streaming source
   - an Iceberg sink
3. Run the Flink job in the current Docker Compose Flink cluster.
4. Create a DBT model that reads the Iceberg table and materializes a simple summary.

## Suggestions

- Add a lightweight Kafka service to `docker-compose.yml` for the streaming source.
- Add Iceberg catalog config, ideally using a local Hadoop catalog or Trino catalog.
- Keep DBT separate from Flink: Flink writes raw/enriched data to Iceberg, DBT does analytical modeling.
- Start with a simple schema and a single output Iceberg table.
- Use local filesystem warehouse path for POC, then swap to a shared object store later.
