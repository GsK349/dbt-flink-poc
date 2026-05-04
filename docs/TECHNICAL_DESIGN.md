# Streaming Lakehouse Pipeline — Technical Design

**Stack:** Apache Flink · dbt · Apache Iceberg · AWS MSK · AWS Glue · S3 · ECS Fargate
**Status:** AWS pilot (branch `aws_plan`)
**Audience:** Engineering leadership, platform engineers, data engineers

---

## 1. Goals & Non-Goals

### Goals
- **Single SQL-defined pipeline** that delivers both real-time and batch transformations.
- **One DAG** spanning Kafka ingestion, Iceberg storage, and downstream marts — with lineage, tests, and docs (dbt).
- **CDC-friendly** — handle inserts, updates, deletes, soft-deletes, and replays with declarative SQL.
- **Self-service per team** — Terraform modules deploy a complete environment from a single `terraform.tfvars` block.
- **Latency knob per model** — same dbt project, choose streaming / incremental / view per output.

### Non-Goals (this iteration)
- Online transactional serving (OLTP). Reads are analytical.
- Sub-100ms latency. Target is sub-second to seconds, not millisecond.
- Multi-region active/active. Single-region pilot.
- Replacing existing batch warehouses where 15-min batches already satisfy SLAs.

---

## 2. High-Level Architecture

```
   ┌────────────┐    ┌─────────────┐    ┌────────────────────┐
   │  Producers │───▶│  AWS MSK    │───▶│  Flink SQL Gateway │
   │  (apps,    │    │  (Kafka)    │    │  on ECS Fargate    │
   │   CDC)     │    └─────────────┘    └─────────┬──────────┘
   └────────────┘                                 │
                                                  ▼
                                       ┌─────────────────────┐
                                       │  Apache Iceberg     │
                                       │  (S3 + Glue catalog)│
                                       └─────────┬───────────┘
                                                 │
                            ┌────────────────────┼────────────────────┐
                            ▼                    ▼                    ▼
                  ┌──────────────────┐  ┌─────────────────┐  ┌──────────────┐
                  │  dbt-flink       │  │  Athena / Trino │  │  Reverse-ETL │
                  │  (ECS task,      │  │  / Spark        │  │  / APIs      │
                  │  EventBridge)    │  │                 │  │              │
                  └──────────────────┘  └─────────────────┘  └──────────────┘
```

**Plane separation:**
- **Streaming plane** (MSK + Flink SQL Gateway) handles continuous ingestion. Long-lived jobs.
- **Transformation plane** (dbt on ECS) handles modeling, projections, and "current state" views. Scheduled.
- **Storage plane** (Iceberg on S3 + Glue) is the source of truth read by all consumers.

---

## 3. Component Inventory

| Component | Purpose | Where it lives |
|-----------|---------|----------------|
| **AWS MSK** | Kafka broker for CDC events | `terraform/modules/msk/` |
| **Flink SQL Gateway** | Submits + executes Flink SQL via REST | `terraform/modules/ecs_sql_gateway/` (ECS Fargate) |
| **MSAF (Managed Service for Apache Flink)** | Long-running streaming jobs (Java/SQL) | `terraform/modules/msaf/` |
| **S3 warehouse** | Iceberg table storage | `terraform/modules/storage/` |
| **Glue catalog** | Iceberg metadata catalog | Same module |
| **dbt-flink-adapter** | Compiles models → Flink SQL | `Dockerfile.dbt`, `dbt/` |
| **EventBridge Scheduler** | Triggers periodic dbt runs | `terraform/modules/ecs_dbt/` |
| **Networking** | VPC, subnets, security groups | `terraform/modules/networking/` |
| **IAM** | Least-privilege roles for Flink + dbt | `terraform/modules/iam/` |

---

## 4. Data Flow

### 4.1 End-to-end for a single record

1. **Producer** writes to OLTP (or CDC connector emits a change event).
2. Event published to MSK topic `orders_topic` with headers `{operation: INSERT|UPDATE|DELETE, source, timestamp}`.
3. **Flink SQL job** (deployed via MSAF or via Gateway) consumes the topic, extracts headers via `METADATA FROM 'value.headers.operation'`, and writes to Iceberg `iceberg_orders_cdc_log` (append-only).
4. **dbt model** `mart_orders_cdc_current` projects the log into "current state" via `ROW_NUMBER() OVER (PARTITION BY id ORDER BY event_ts DESC) WHERE rn = 1 AND op <> 'DELETE'`.
5. **Consumer** queries the dbt view via Athena/Trino → reads the latest Iceberg snapshot → sees the change within seconds.

**Latency budget (target):**
- OLTP → Kafka: <100 ms (CDC connector dependent)
- Kafka → Iceberg: 1–10 s (Flink checkpoint interval = 10s)
- Iceberg snapshot visible to query: immediate after commit
- **End-to-end p95: ~5–10 seconds**

### 4.2 Why append-only + projection

The CDC log is **append-only**. We never UPDATE or DELETE Iceberg rows in the streaming path:

- Iceberg streaming writes are most reliable as inserts (no MERGE contention).
- Full history is preserved → time travel and replays are deterministic.
- "Current state" is a window function over the log, not a separate table.
- Soft deletes (e.g. GDPR) are also expressed as filter predicates, not row-deletes.

Trade-off: queries pay a window cost. Mitigated by partition pruning on `event_ts` and by promoting hot marts to incremental materializations.

---

## 5. Repository Layout

```
flink_dbt_poc/
├── dbt/
│   ├── macros/
│   │   ├── cdc_current_state.sql      # CDC log → current state
│   │   ├── soft_delete_active.sql     # Soft-delete table → active rows
│   │   └── setup_iceberg_catalog.sql  # on-run-start hook
│   ├── models/
│   │   ├── stg_orders.sql             # View over raw Iceberg table
│   │   ├── mart_orders_cdc_current.sql# Current state via macro
│   │   ├── mart_orders_active.sql     # Soft-delete active rows
│   │   └── mart_orders_summary.sql    # Daily aggregate
│   └── profiles.yml                   # dev (local) + aws (gateway) targets
├── sql/                               # Hand-deployed Flink SQL jobs
│   ├── flink_kafka_to_iceberg.sql
│   ├── streaming_cdc_op_field.sql
│   ├── streaming_upsert_primary_key.sql
│   ├── streaming_soft_delete.sql
│   ├── batch_s3_partitioned_parquet.sql
│   └── iceberg_updates_examples.sql
├── scripts/
│   ├── start_flink_sql_gateway.sh
│   ├── run_sql_via_gateway.sh         # Submit SQL file to gateway REST API
│   ├── create_msk_topics.py           # IAM-SASL authenticated topic creation
│   ├── apply_data_modifications.py    # Periodic UPDATE/DELETE batches
│   └── deploy_msaf_job.sh
├── terraform/
│   ├── main.tf                        # Wires modules together
│   ├── variables.tf
│   ├── terraform.tfvars               # Per-team config
│   └── modules/
│       ├── networking/                # VPC, subnets, SGs
│       ├── iam/                       # Roles for Flink + dbt + MSAF
│       ├── storage/                   # S3 warehouse + Glue DB
│       ├── msk/                       # Kafka cluster + topics
│       ├── msaf/                      # Managed Flink for long-running jobs
│       ├── ecs_sql_gateway/           # Flink SQL Gateway (ALB + Fargate)
│       └── ecs_dbt/                   # Scheduled dbt runner
├── Dockerfile.flink                   # Gateway image (Flink 1.20 + Iceberg jars)
├── Dockerfile.dbt                     # dbt-core 1.3.7 + dbt-flink-adapter 1.3.11
├── dbt_project.yml
└── docker-compose.yml                 # Local dev stack
```

---

## 6. Streaming Layer (Flink)

### 6.1 Two execution modes

| Mode | When | How |
|------|------|-----|
| **MSAF (Managed Apache Flink)** | Long-lived production streams | JAR or SQL artifact in S3, deployed by `terraform/modules/msaf` |
| **SQL Gateway on ECS** | Interactive dev, ad-hoc queries, dbt deployment target | `terraform/modules/ecs_sql_gateway` |

Both share the same Iceberg + Glue catalog, so a job started one way is queryable from the other.

### 6.2 SQL Gateway internals

- Image: `apache/flink:1.20.1-scala_2.12` + Iceberg + AWS Glue + S3 jars (`Dockerfile.flink`).
- Container exposes port `8083` (REST API).
- ECS Fargate task: 2 vCPU / 4 GB memory, single replica with autoscaling 1–5 on CPU>60%.
- Internal ALB on port 8083 in private subnets.
- `taskmanager.numberOfTaskSlots: 4` per gateway pod.

Submit a SQL file:
```bash
GATEWAY=http://<alb-dns>:8083 \
  ./scripts/run_sql_via_gateway.sh --file sql/flink_kafka_to_iceberg.sql
```

### 6.3 Kafka source pattern (with headers)

We read CDC operation from the Kafka **header**, not the payload. Keeps the data schema clean and matches Debezium/Confluent conventions:

```sql
CREATE TABLE orders_cdc_with_headers (
  id STRING, customer_id STRING, order_ts TIMESTAMP(3),
  amount DECIMAL(10,2), status STRING,
  operation STRING METADATA FROM 'value.headers.operation',
  source    STRING METADATA FROM 'value.headers.source',
  event_timestamp STRING METADATA FROM 'value.headers.timestamp',
  WATERMARK FOR order_ts AS order_ts - INTERVAL '0' SECOND
) WITH (
  'connector' = 'kafka',
  'topic' = 'orders_topic',
  'properties.bootstrap.servers' = '${KAFKA_BOOTSTRAP_SERVERS}',
  'scan.startup.mode' = 'earliest-offset',
  'format' = 'json'
);
```

### 6.4 Iceberg sink pattern

```sql
CREATE CATALOG iceberg_catalog WITH (
  'type'         = 'iceberg',
  'warehouse'    = '${ICEBERG_WAREHOUSE_PATH}',
  'catalog-impl' = 'org.apache.iceberg.aws.glue.GlueCatalog',
  'io-impl'      = 'org.apache.iceberg.aws.s3.S3FileIO'
);

CREATE TABLE iceberg_catalog.`default`.iceberg_orders_cdc_log (
  id STRING, customer_id STRING, order_ts TIMESTAMP(3),
  amount DECIMAL(10,2), status STRING,
  operation STRING, source STRING, event_timestamp STRING,
  ingested_at TIMESTAMP(3)
) WITH ('write.format.default' = 'parquet');

INSERT INTO iceberg_catalog.`default`.iceberg_orders_cdc_log
SELECT id, customer_id, order_ts, amount, status,
       operation, source, event_timestamp, CURRENT_TIMESTAMP
FROM   default_catalog.default_database.orders_cdc_with_headers
WHERE  operation IS NOT NULL;
```

### 6.5 Checkpointing & exactly-once

```sql
SET 'execution.runtime-mode'                = 'streaming';
SET 'execution.checkpointing.interval'      = '10s';
SET 'execution.checkpointing.mode'          = 'EXACTLY_ONCE';
```

Iceberg's commit protocol participates in Flink checkpoints — every successful checkpoint produces a new Iceberg snapshot. Replays from Kafka offsets + idempotent Iceberg commits give effective exactly-once for the sink path.

---

## 7. Transformation Layer (dbt)

### 7.1 Project layout

- **Profile** `flink_iceberg` — two targets:
  - `dev` — `localhost:8083` (Docker compose stack)
  - `aws` — gateway ALB DNS via `FLINK_GATEWAY_HOST` env
- **`on-run-start`** hook calls `setup_iceberg_catalog()` to register the Glue-backed Iceberg catalog at run time.

### 7.2 Macros (the core of the low-code story)

**`cdc_current_state(source_table, key_columns, extra_columns, ...)`**
Generic CDC-log → current-state projection. Supports composite keys.

```sql
{% macro cdc_current_state(source_table, key_columns, extra_columns,
                            order_column='event_ts',
                            op_column='op', delete_value='DELETE') %}
SELECT {{ key_columns | join(', ') }}, {{ extra_columns | join(', ') }},
       {{ op_column }} AS operation, {{ order_column }} AS last_modified_at
FROM (
  SELECT {{ key_columns | join(', ') }}, {{ extra_columns | join(', ') }},
         {{ op_column }}, {{ order_column }},
         ROW_NUMBER() OVER (PARTITION BY {{ key_columns | join(', ') }}
                            ORDER BY {{ order_column }} DESC) AS rn
  FROM {{ source_table }}
  WHERE {{ op_column }} <> '{{ delete_value }}'
)
WHERE rn = 1
{% endmacro %}
```

**`soft_delete_active(source_table, columns, deleted_column='is_deleted')`**
Generic soft-delete filter.

**Adding a new CDC table = one 5-line model:**

```sql
{{ config(materialized='view') }}
{{ cdc_current_state(
    source_table="iceberg_catalog.`default`.iceberg_customers_cdc_log",
    key_columns=['customer_id'],
    extra_columns=['name', 'email', 'tier']
) }}
```

### 7.3 Materialization strategies (the latency knob)

| Materialization | Freshness | Cost | Use for |
|---|---|---|---|
| `view` (default) | Seconds (fresh-on-read) | $ — no compute until queried | Most marts, BI projections |
| `incremental` | Minutes (run cadence) | $$ — periodic compute | SCD2, hourly aggregates |
| `table` + `execution_mode='streaming'` | Sub-second | $$$ — always-on Flink slot | Live enrichments, alerting features |

The same dbt project can mix all three. Promote a model from `view` to `streaming` by changing the `config()` block — no code rewrite.

### 7.4 Streaming materialization caveats

dbt's mental model assumes runs complete. Streaming jobs don't:
- `dbt run` twice → two streaming jobs unless you guard for it.
- `--full-refresh` may kill a multi-week-old stateful job.
- **Mitigation:** wrapper script that checks gateway for an existing job at the same SQL hash before issuing the INSERT. Track deployed job IDs in a small `streaming_jobs` registry table.

---

## 8. Storage Layer (Iceberg)

### 8.1 Why Iceberg
- ACID on object storage (S3) without Hive metastore.
- Schema evolution (add/drop/rename columns) without rewriting data.
- Time travel via snapshot IDs / timestamps.
- Hidden partitioning (transforms applied automatically, no partition column maintenance).
- Multi-engine reads: Flink, Trino, Athena, Spark, dbt all share the same tables.

### 8.2 Catalog choice
**AWS Glue catalog** — managed, IAM-authorized, cross-account-shareable. Avoids running Hive Metastore.

### 8.3 Table conventions
| Naming | Purpose |
|---|---|
| `iceberg_<entity>` | Latest-state table (rare; usually a CDC log instead) |
| `iceberg_<entity>_cdc_log` | Append-only CDC events |
| `iceberg_<entity>_soft_delete` | Soft-deletable mutable table |
| `mart_<name>` | dbt-managed downstream view/table |

### 8.4 Compaction (operational must-have)
Streaming writes produce many small files. Set up a periodic compaction job:
- `CALL iceberg_catalog.system.rewrite_data_files('default.iceberg_orders_cdc_log')`
- Run hourly or daily depending on volume.
- Schedule via EventBridge → ECS task or via Spark on EMR Serverless.

---

## 9. AWS Deployment (Terraform)

### 9.1 Module dependency graph

```
networking ──┬──▶ msk
             ├──▶ ecs_sql_gateway ──┐
             ├──▶ ecs_dbt ──────────┤
             └──▶ msaf              │
                                    ▼
iam ────────────────────────▶ storage (S3 + Glue)
```

### 9.2 Self-service variables (`terraform.tfvars`)

```hcl
team_name   = "sai-flink"
aws_region  = "us-east-1"
aws_account = "<account-id>"

vpc_cidr           = "10.1.0.0/16"
availability_zones = ["us-east-1a", "us-east-1b", "us-east-1c"]

kafka_instance_type        = "kafka.m5.large"
kafka_broker_count         = 3
kafka_topics               = ["orders_topic"]
kafka_partitions_per_topic = 6

flink_parallelism         = 6
flink_parallelism_per_kpu = 1

s3_warehouse_bucket = "<team>-flink-iceberg-warehouse-<account>"
glue_database_name  = "default"

dbt_schedule = "rate(15 minutes)"
```

### 9.3 SQL Gateway service (`ecs_sql_gateway/`)
- ECR repo `<team>/flink-sql-gateway`
- ECS cluster `<team>-cluster` with Container Insights
- Fargate task: 2 vCPU / 4 GB
- Internal ALB on port 8083, target group health check `GET /v1/info`
- Application Auto Scaling: 1–5 replicas, target CPU 60%
- CloudWatch logs `/ecs/<team>/sql-gateway`, 7-day retention

### 9.4 IAM minimum permissions
- **Flink role**: read MSK (IAM-SASL), read/write S3 warehouse prefix, Glue read+write on `glue_database_name`
- **dbt role**: same as Flink for catalog operations + invoke gateway (HTTP, no AWS API)
- **ECS execution role**: ECR pull, CloudWatch Logs

### 9.5 MSK authentication
IAM SASL (no static credentials):
- Producers/consumers use `aws-msk-iam-sasl-signer-python`.
- Bootstrap servers = `bootstrap.kafka.<region>.amazonaws.com:9098`.

---

## 10. Operational Patterns

### 10.1 Local development
```bash
docker compose up
# brings up: zookeeper, kafka, flink-jobmanager, flink-taskmanager, sql-gateway, dbt
```
Then:
```bash
./scripts/run_sql_via_gateway.sh --file sql/flink_kafka_to_iceberg.sql
docker compose run dbt run --target dev
```

### 10.2 CI/CD
- `dbt parse` on every PR (no DB needed) — catches Jinja/macro errors.
- `dbt compile` to render SQL for code review.
- `terraform plan` per module against pilot account.
- Image build on merge → push to ECR → force ECS service deploy.

### 10.3 Monitoring & alerting
- **Flink job health:** CloudWatch metrics from MSAF (`numRecordsIn`, `numLateRecords`, `checkpointDuration`).
- **MSK consumer lag:** `MaxOffsetLag` per consumer group.
- **dbt run results:** parse `target/run_results.json`, ship to CloudWatch as custom metric.
- **Iceberg snapshot rate:** log per-table snapshot count, alert if a table stops receiving snapshots.

### 10.4 Disaster scenarios

| Scenario | Behavior |
|---|---|
| SQL Gateway pod crash | ECS replaces; in-flight session SQL must be re-submitted |
| MSK broker failure | Producers retry; consumers rebalance; no data loss with `acks=all` |
| Iceberg sink commit fails | Flink checkpoint fails → retry from last good checkpoint |
| dbt run fails | Next scheduled run picks up; views are idempotent |
| Streaming model SQL bug | Job runs but emits bad rows; fix SQL + redeploy + (optionally) reset Kafka offsets and rebuild downstream |

### 10.5 Data correction patterns
- **Wrong rows in CDC log** → emit a corrective `UPDATE` event with newer timestamp; projection picks the latest automatically.
- **Schema column added** → Iceberg evolves in place; backfill old rows with `ALTER TABLE ... SET DEFAULT`.
- **Replay from N hours ago** → reset Kafka consumer offsets, rerun streaming job. Iceberg dedup not automatic — design CDC log to tolerate replays (use event timestamp, not ingestion timestamp, in projections).

---

## 11. Security

- **Network:** all compute in private subnets. ALB internal-only. NAT for egress only.
- **Encryption:** S3 SSE-KMS, MSK in-transit TLS + IAM, Iceberg metadata in Glue (KMS-encryptable).
- **Secrets:** none stored in repo. MSK uses IAM SASL. Iceberg uses task role.
- **Access:** Glue catalog read/write controlled by IAM roles per team. Athena workgroup per team.
- **PII / GDPR:** see `GDPR_DATA_RETENTION.md` — soft-delete pattern + scheduled purge job using Iceberg `delete_files`.

---

## 12. Cost Model

| Component | Cost shape | Drivers |
|---|---|---|
| MSK | Always-on, per broker-hour | Broker count, instance type, storage |
| ECS Fargate (gateway) | Always-on, per vCPU/memory-second | Replicas, task size |
| ECS Fargate (dbt) | Per run, ~minutes | Run cadence (15 min default) |
| MSAF | Per KPU-hour, always-on | `flink_parallelism / flink_parallelism_per_kpu` |
| S3 storage | Per GB-month | Iceberg data + metadata; compaction reduces |
| S3 requests | Per million GET/PUT | Streaming snapshot rate |
| Glue catalog | Per million requests | Catalog ops (small) |
| Athena | Per TB scanned | Query patterns; Iceberg partition pruning helps |
| CloudWatch | Per GB ingested | Log verbosity, retention |

**Largest cost lever:** count of streaming-materialized models (always-on Flink slots). Default to `view` materialization, promote on evidence.

---

## 13. Roadmap

| Phase | Scope | Status |
|---|---|---|
| 1 — POC | Local Flink + dbt + Iceberg, single CDC source | Done |
| 2 — AWS pilot | MSK + ECS gateway + scheduled dbt + Terraform modules | In progress (`aws_plan` branch) |
| 3 — Config-driven | `tables.yml` + generator macros, new table = 5-line config | Planned |
| 4 — Streaming materializations | Selectively promote marts; lifecycle tooling | Planned |
| 5 — Multi-tenant | Self-service Terraform per team; shared warehouse | Planned |
| 6 — Compaction & retention automation | Scheduled `rewrite_data_files`, snapshot expiration | Planned |

---

## 14. Open Questions / Decisions Needed

- **Streaming job lifecycle tool** — bespoke wrapper vs Flink Kubernetes Operator vs commercial (Decodable/Aiven)?
- **Compaction owner** — Spark on EMR Serverless vs Flink batch job vs Iceberg auto-compaction?
- **Schema registry** — adopt Confluent/AWS Glue Schema Registry to enforce contracts on Kafka topics?
- **Monitoring stack** — CloudWatch only, or add Prometheus + Grafana for Flink internals?
- **Governance** — Lake Formation tags vs IAM-only access control?

---

## 15. References (in-repo)

- `README.md` — quickstart
- `PIPELINE_EXPLAINER.md` — narrative walkthrough
- `STREAMING_CDC_UPSERTS.md` — CDC pattern deep dive
- `KAFKA_HEADERS_FOR_CDC.md` — header-driven CDC
- `ICEBERG_UPDATES_DELETES.md` — mutable table patterns
- `BATCH_PROCESSING_GUIDE.md` — batch S3 → Iceberg
- `SCALABLE_DATA_MODIFICATIONS.md` — periodic UPDATE/DELETE batches
- `GDPR_DATA_RETENTION.md` — PII handling
- `MIGRATION_TO_S3_BATCH.md` — migration runbook
- `AWS_PIPELINE_RUN_REPORT.md` — last successful AWS run
- `DEBUGGING_SUMMARY.md` — known issues & fixes
- `docs/presentation.html` — leadership-facing deck

---

## 16. Glossary

- **CDC** — Change Data Capture. Stream of inserts/updates/deletes from a source system.
- **MSAF** — Amazon Managed Service for Apache Flink (formerly Kinesis Data Analytics).
- **MSK** — Amazon Managed Streaming for Apache Kafka.
- **KPU** — Kinesis Processing Unit. Flink billing unit on MSAF (1 vCPU + 4 GB).
- **Iceberg snapshot** — Immutable point-in-time view of a table; produced on every write.
- **Watermark** — Flink's notion of "all events up to T have arrived" for windowed operators.
- **Materialization** (dbt) — How a model is persisted: view, table, incremental, or adapter-specific (e.g., streaming).
