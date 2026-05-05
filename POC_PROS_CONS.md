# Proof of Concept — Final Report

**Project:** Unified Streaming + Batch Data Pipeline on Iceberg
**Stack:** Apache Flink · dbt · Apache Iceberg · AWS Glue · MSK · S3
**Author:** Data Platform Team
**Date:** 2026-05-05
**Document version:** 1.0
**Status:** Complete — Awaiting Decision

---

## 1 · Executive Summary

This Proof of Concept evaluates whether a unified pipeline combining **Apache Flink** (compute), **dbt** (transformation framework), and **Apache Iceberg** (table format) can replace or augment our current Spark-on-Glue data architecture.

**Verdict: Architecturally validated. Operationally not yet production-ready.**

The PoC successfully demonstrates that streaming and batch workloads can converge on a single SQL surface backed by an open table format. However, several operational gaps and adapter-level immaturities mean that a wholesale production rollout is premature. A phased adoption — starting with dbt on the existing Spark stack — is recommended.

| Decision Point | Recommendation |
|---|---|
| Adopt dbt as the transformation framework | ✅ Proceed |
| Replace Spark Glue jobs with Flink (wholesale) | ❌ Hold |
| Adopt Flink for low-latency / stateful workloads only | ✅ Targeted pilot |
| Standardize on Iceberg + Glue catalog | ✅ Proceed |
| Production rollout of this exact PoC stack | ⚠️ Hardening required first |

---

## 2 · Objectives & Scope

### 2.1 Objectives

- Validate that streaming and batch pipelines can share a single declarative SQL codebase
- Prove Iceberg viability as the unified table format for both workloads
- Assess dbt as a developer interface for Flink-based pipelines
- Quantify the trade-offs against the current Spark-on-Glue baseline
- Surface operational risks before committing to production

### 2.2 In Scope

- Streaming ingestion: MSK → Flink → Iceberg  log
- Batch ingestion: S3 Parquet → Flink → Iceberg tables
- Transformation layer: dbt models, tests, lineage, docs
- Local development experience
- Connector and adapter capability assessment


---

## 3 · What Was Built

### 3.1 Infrastructure
- Docker Compose stack with Kafka, Zookeeper, Flink JobManager, TaskManager, SQL Gateway
- AWS deployment artifacts targeting Glue catalog and S3 warehouse
- Connector libraries (Iceberg, S3FileIO, Kafka, AWS) packaged into the Flink image

### 3.2 Pipelines (9 SQL scripts)
- Streaming: CDC with operation field, soft-delete, primary-key upsert, Kafka headers as metadata
- Batch: S3 Parquet ingestion (partitioned and unpartitioned), Iceberg updates and deletes
- Combined: stream + batch unified into Iceberg

### 3.3 dbt Project
- 5 models (1 staging, 3 marts, 1 incremental fact)
- 3 reusable macros (CDC current-state, soft-delete, Iceberg catalog setup)
- 3 sources with freshness checks and column-level tests
- 2 singular tests, 1 custom generic test
- 1 SCD2 snapshot, 1 seed file, 1 analysis
- 3 declared exposures (dashboard, GDPR notebook, ML model)
- 28+ generic tests (`unique`, `not_null`, `accepted_values`, custom)

### 3.4 Documentation
- `README.md` — quickstart and architecture summary
- `STREAMING_CDC_UPSERTS.md` — three CDC patterns explained
- `BATCH_PROCESSING_GUIDE.md` — batch ingest semantics
- `ICEBERG_UPDATES_DELETES.md` — connector capability matrix
- `KAFKA_HEADERS_FOR_CDC.md` — metadata column patterns
- `SCALABLE_DATA_MODIFICATIONS.md`, `MIGRATION_TO_S3_BATCH.md`
- `docs/TECHNICAL_DESIGN.md` — full system design

---

## 4 · Findings

### 4.1 Strengths

#### Architecture
- One SQL surface targets both streaming and batch via materialization config
- Open formats end-to-end (Iceberg + Parquet + Glue) — no vendor lock-in
- ACID transactions and time-travel queries on the lake
- Append-only CDC log enables unlimited replay and simplifies writers
- Schema and lineage are version-controlled in Git

#### Developer Experience
- Analysts can author streaming pipelines using SQL alone
- Pull-request workflow for streaming jobs (review, test, merge)
- Local stack ready in under 2 minutes via `docker-compose up`
- Single shared `profiles.yml` removes per-developer config drift
- `dbt build` + tests gate merges on data quality

#### Data Quality & Governance
- `dbt test` covers streaming sinks, not just batch marts
- Iceberg time-travel reconstructs any past state for compliance
- CDC headers preserved as metadata columns for full provenance
- GDPR pattern designed in: soft-delete → batch hard-purge → audit log
- Lineage graph spans Kafka → Iceberg → marts in a single view

#### Operational
- EXACTLY_ONCE end-to-end with 10-second checkpoints
- Streaming and batch workloads can run on isolated TaskManager pools
- Parquet on S3 is materially cheaper than warehouse-native storage
- Pay-per-query on the read side via Athena or Trino

### 4.2 Limitations

#### Connector & Adapter Maturity
- **Flink-Iceberg 1.10.1** does not implement `SupportsRowLevelUpdate/Delete` — streaming UPDATE/DELETE statements are rejected
- **dbt-flink** source declarations require explicit `data_type` per column

#### Operational Gaps
- No compaction job exists — small files accumulate from 10s checkpoints
- No monitoring or alerting plumbing wired in
- Decision between MSAF (managed) and ECS SQL Gateway (self-hosted) is unresolved
- Backfills can collide with live streaming if both write the same table
- No registry or locking for streaming dbt models

#### Developer Friction
- dbt-flink error messages surface as opaque SQL Gateway stack traces
- `dbt run` confirms job *submission* but not *output* — operators need a separate health check
- Local Iceberg testing requires either shared Glue (blast radius) or a divergent local Hadoop catalog
- Snapshots and seeds need adapter workarounds

#### Untested Risks
- Streaming failure modes (backpressure, watermark stalls, OOM) not stress-tested
- Recovery time after a failure is unmeasured
- 10× peak Kafka throughput not exercised
- No Schema Registry integration evaluated

---

## 5 · Comparison vs Status Quo

| Capability | Current (Spark + Glue) | PoC (Flink + dbt + Iceberg) |
|---|---|---|
| Streaming pipelines | PySpark on Glue, 24/7 | Declarative SQL via dbt PR |
| Batch pipelines | PySpark on Glue, scheduled | Flink batch + dbt, same SQL surface |
| Compaction | Implemented as a Glue job | **Not yet built** |
| Storage format | Iceberg (already adopted) | Iceberg (no change) |
| Lineage | Tribal / tool-specific | Single `dbt docs` graph |
| Time travel | Native to Iceberg | Native to Iceberg |
| GDPR deletion | Custom Python scripts | Standardized soft → hard pattern |
| Stream + batch sharing code | No | Yes |
| Streaming latency | Spark micro-batch (~30s+) | Flink true-streaming (sub-second possible) |
| Operator skill required | PySpark | Flink + Iceberg + dbt-adapter |

---

## 6 · Risk Register

| # | Risk | Likelihood | Impact | Mitigation |
|---|---|---|---|---|
| R1 | Row-level DML blocked in streaming | High | High | Use raw/clean table split or upgrade connector |
| R2 | Small-file proliferation | High | Medium | Build scheduled compaction job before launch |
| R3 | Streaming job duplicated by re-run | Medium | High | Build job registry + idempotent submission |
| R4 | Backfill collides with live stream | Medium | High | Enforce `_raw` / `_clean` separation |
| R5 | dbt-flink adapter regressions | Medium | Medium | Pin versions; maintain shadow dbt-spark profile |
| R6 | Operator skill gap | High | Medium | Training program; gradual handover from platform team |
| R7 | Cost overrun | Medium | Medium | Build cost model before broad rollout |
| R8 | Schema drift breaks sinks | Medium | Medium | Adopt Schema Registry before multi-producer expansion |

---

## 7 · Recommendations

### 7.1 Adopt
- **dbt as the transformation framework**, even on the current Spark stack — captures most of the developer-experience and governance wins independent of the engine choice
- **Iceberg as the standard table format** — already in use; continue
- **Glue catalog as the metastore** — standard, low-friction
- **The soft-delete → batch-purge GDPR pattern** — well-designed, low-risk

### 7.2 Pilot (Targeted Use Cases Only)
- **Flink streaming** — for workloads requiring sub-10s latency or stateful windowing (CEP, session windows, interval joins) that Spark Structured Streaming cannot meet
- **Flink batch** — for workloads where unified code with the streaming side is genuinely valuable

### 7.3 Hold
- **Wholesale Glue → Flink migration** — risk does not justify the reward given the working Spark stack
- **dbt streaming materialization in production** — until the adapter implements idempotent submission

### 7.4 Defer
- **MSAF vs ECS Gateway decision** — pending the targeted Flink pilot results
- **Schema Registry integration** — until multi-producer scenarios become real
- **Multi-region setup** — out of PoC scope

---

## 8 · Recommended Next Steps

| # | Action | Owner | Priority | Target |
|---|---|---|---|---|
| 1 | Upgrade Flink-Iceberg connector for row-level UPDATE/DELETE | Platform | High | Q3 2026 |
| 2 | Build scheduled Iceberg compaction job (`rewrite_data_files`) | Platform | High | Q3 2026 |
| 3 | Add job registry + idempotent submission to dbt-flink workflow | Platform | High | Q3 2026 |
| 4 | Decide MSAF vs ECS Gateway; write runbook | Platform | Medium | Q3 2026 |
| 5 | Wire metrics & alerts (Prometheus, Grafana, dbt freshness) | SRE | High | Q3 2026 |
| 6 | Stress test: 10× peak Kafka, TaskManager kills, schema changes | Platform + SRE | Medium | Q4 2026 |
| 7 | Build cost model at expected steady-state and peak | Finance + Platform | Medium | Q3 2026 |
| 8 | Plan Schema Registry integration | Platform | Low | Q4 2026 |
| 9 | Adopt dbt-spark on existing Glue stack (Phase 1) | Analytics + Platform | High | Q3 2026 |
| 10 | Identify candidate workloads for Flink targeted pilot | Product + Platform | Medium | Q3 2026 |

---

## 9 · Decision Required

The sponsoring committee is asked to approve:

- [ ] **Recommendation 7.1** — Adopt dbt + Iceberg + Glue as standards
- [ ] **Recommendation 7.2** — Sanction a targeted Flink pilot for low-latency / stateful workloads
- [ ] **Recommendation 7.3** — Reject wholesale Glue → Flink migration at this time
- [ ] **Next Steps 1–10** — Allocate engineering capacity for the hardening phase

---

## 10 · Appendices

- **Appendix A** — Detailed test matrix and pilot run results: `AWS_PIPELINE_RUN_REPORT.md` (19 numbered issues)
- **Appendix B** — Technical design and component-level specs: `docs/TECHNICAL_DESIGN.md`
- **Appendix C** — Streaming CDC patterns: `STREAMING_CDC_UPSERTS.md`, `KAFKA_HEADERS_FOR_CDC.md`
- **Appendix D** — Batch processing semantics: `BATCH_PROCESSING_GUIDE.md`, `MIGRATION_TO_S3_BATCH.md`
- **Appendix E** — Iceberg connector capabilities: `ICEBERG_UPDATES_DELETES.md`, `SCALABLE_DATA_MODIFICATIONS.md`
- **Appendix F** — GDPR retention workflow: `GDPR_DATA_RETENTION.md`
- **Appendix G** — Animated architecture deck: `pipeline-presentation.html`
- **Appendix H** — Runnable end-to-end demo: `notebooks/poc_demo.ipynb`
- **Appendix I** — Debugging notes from PoC build: `DEBUGGING_SUMMARY.md`

---

## 11 · Sign-off

| Role | Name | Decision | Date |
|---|---|---|---|
| Sponsor | _____________ | ☐ Approved &nbsp; ☐ Rejected &nbsp; ☐ Revisions Requested | _____ |
| Engineering Lead | _____________ | ☐ Approved &nbsp; ☐ Rejected &nbsp; ☐ Revisions Requested | _____ |
| Data Platform Lead | _____________ | ☐ Approved &nbsp; ☐ Rejected &nbsp; ☐ Revisions Requested | _____ |
| Analytics Lead | _____________ | ☐ Approved &nbsp; ☐ Rejected &nbsp; ☐ Revisions Requested | _____ |
| Compliance / DPO | _____________ | ☐ Approved &nbsp; ☐ Rejected &nbsp; ☐ Revisions Requested | _____ |

---

*End of report.*
