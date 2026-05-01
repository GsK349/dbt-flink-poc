# Pipeline Resiliency Analysis

Based on all failure modes observed across debugging sessions on this Flink + Iceberg + dbt pipeline.
Every finding here is tied to something that actually broke, not theoretical risk.

---

## Gap 1 — Streaming Jobs Die on Any Container Restart

**Severity:** Critical

### What Broke

Running `docker compose up -d sql-gateway` silently recreated the jobmanager container,
killing all 3 streaming jobs with zero warning or error. Manual re-submission of all 3
SQL files was required to restore the pipeline.

### Why It's Bad

Any routine infra operation — OOM kill, rolling deploy, node replacement, accidental
`docker compose up` — wipes all running jobs. There is no automatic recovery. The pipeline
goes dark silently.

### Fix 1 — Savepoints Before Any Restart

The jobs already use EXACTLY_ONCE checkpointing. Add a shutdown script that takes a
savepoint before stopping, and a startup script that restarts from it.

```bash
#!/bin/bash
# scripts/graceful_stop.sh — run before any docker compose restart

JOBS=$(curl -s http://localhost:8081/jobs | python3 -c "
import json, sys
for j in json.load(sys.stdin)['jobs']:
    if j['status'] == 'RUNNING':
        print(j['id'])
")

for JOB_ID in $JOBS; do
  echo "Stopping job $JOB_ID with savepoint..."
  curl -s -X POST "http://localhost:8081/jobs/$JOB_ID/stop" \
    -H "Content-Type: application/json" \
    -d '{"drain": false, "targetDirectory": "s3://flink-iceberg-warehouse/savepoints/"}'
done
```

Then on restart, pass the savepoint path to the INSERT job via
`SET 'execution.savepoint.path' = 's3://...'` before the INSERT statement.

### Fix 2 — Idempotent Job Submission Script

```bash
#!/bin/bash
# scripts/ensure_jobs_running.sh

EXPECTED_JOBS=(
  "streaming_upsert_primary_key.sql"
  "streaming_cdc_op_field.sql"
  "streaming_soft_delete.sql"
)

RUNNING=$(curl -s http://localhost:8081/jobs | python3 -c "
import json, sys
jobs = json.load(sys.stdin)['jobs']
print(sum(1 for j in jobs if j['status'] == 'RUNNING'))
")

echo "Running jobs: $RUNNING / ${#EXPECTED_JOBS[@]}"

if [ "$RUNNING" -lt "${#EXPECTED_JOBS[@]}" ]; then
  echo "Jobs missing — resubmitting..."
  for SQL_FILE in "${EXPECTED_JOBS[@]}"; do
    docker cp sql/$SQL_FILE flink_dbt_poc-jobmanager-1:/tmp/$SQL_FILE
    docker exec -d flink_dbt_poc-jobmanager-1 \
      /opt/flink/bin/sql-client.sh -f /tmp/$SQL_FILE
    echo "Submitted $SQL_FILE"
  done
fi
```

Run this after every `docker compose up` and as a cron job every 5 minutes.

---

## Gap 2 — AWS Credentials Are Ephemeral

**Severity:** Critical

### What Broke

After `docker compose up`, both jobmanager and sql-gateway started with empty
`AWS_ACCESS_KEY_ID`. Every Iceberg/Glue operation failed:

```
SdkClientException: Unable to load credentials from any of the providers in the chain
AwsCredentialsProviderChain(...) : [EnvironmentVariableCredentialsProvider(): Unable to
load credentials from system settings. Access key must be specified either via environment
variable (AWS_ACCESS_KEY_ID) or system property (aws.accessKeyId)...]
```

The credentials are passed as env vars from the host shell at compose time. Any automated
restart (cron, health-check-triggered, CI/CD) that runs without the credentials exported
will start containers silently broken.

### Fix — Mount `~/.aws` as a Read-Only Volume

In `docker-compose.yml`, replace the env var blocks on jobmanager, taskmanager, and
sql-gateway with a volume mount:

```yaml
# REMOVE this from each service:
environment:
  AWS_ACCESS_KEY_ID: ${AWS_ACCESS_KEY_ID}
  AWS_SECRET_ACCESS_KEY: ${AWS_SECRET_ACCESS_KEY}
  AWS_SESSION_TOKEN: ${AWS_SESSION_TOKEN}

# ADD this to each service instead:
volumes:
  - $HOME/.aws:/root/.aws:ro
```

The AWS SDK checks `~/.aws/credentials` as one of its default provider chain locations.
This survives container restarts without any shell state. On EC2/ECS, replace with an
IAM instance profile and remove credentials entirely.

### Workaround (Until Fix Is Applied)

Always run this before any `docker compose` command:

```bash
export AWS_ACCESS_KEY_ID=$(grep aws_access_key_id ~/.aws/credentials | head -1 | awk '{print $3}')
export AWS_SECRET_ACCESS_KEY=$(grep aws_secret_access_key ~/.aws/credentials | head -1 | awk '{print $3}')
export AWS_SESSION_TOKEN=""
docker compose up -d
```

---

## Gap 3 — Duplicate Streaming Jobs with No Guard

**Severity:** High

### What Broke

Two jobs were found simultaneously writing to `iceberg_orders_cdc_log`:

```
e9f75fa9a0fe62c1  RUNNING  insert-into_iceberg_catalog.default.iceberg_orders_cdc_log
3e666d3a726555ca  RUNNING  insert-into_iceberg_catalog.default.iceberg_orders_cdc_log
```

Both consumed from the same Kafka topic starting at `earliest-offset`. Every Kafka message
was written twice to the append-only Iceberg table. The Flink REST API has no job-name
uniqueness check — submitting the same SQL file twice silently creates a second job.

### Why It's Bad

For append-only tables (CDC log), duplicates are invisible and permanent. For upsert tables
the impact is lower (idempotent), but double the write throughput wastes slots and I/O.

### Fix 1 — Name Each Job

Add a stable pipeline name to each SQL file:

```sql
-- At the top of streaming_cdc_op_field.sql
SET 'pipeline.name' = 'cdc-log-pipeline';

-- At the top of streaming_upsert_primary_key.sql
SET 'pipeline.name' = 'upsert-pipeline';

-- At the top of streaming_soft_delete.sql
SET 'pipeline.name' = 'soft-delete-pipeline';
```

### Fix 2 — Duplicate Guard in Submission Script

```bash
#!/bin/bash
# Check by pipeline name before submitting

PIPELINE_NAME=$1
SQL_FILE=$2

ALREADY_RUNNING=$(curl -s http://localhost:8081/jobs/overview | python3 -c "
import json, sys
jobs = json.load(sys.stdin).get('jobs', [])
running = [j for j in jobs if j['status'] == 'RUNNING' and '$PIPELINE_NAME' in j.get('name','')]
print(len(running))
")

if [ "$ALREADY_RUNNING" -gt 0 ]; then
  echo "SKIP: $PIPELINE_NAME is already running"
  exit 0
fi

echo "Submitting $PIPELINE_NAME..."
docker cp $SQL_FILE flink_dbt_poc-jobmanager-1:/tmp/$(basename $SQL_FILE)
docker exec -d flink_dbt_poc-jobmanager-1 \
  /opt/flink/bin/sql-client.sh -f /tmp/$(basename $SQL_FILE)
```

---

## Gap 4 — SQL Gateway Is a Single Point of Failure

**Severity:** High

### What Broke

The SQL Gateway crashed repeatedly. Issues observed:

1. **Missing `address` config** — Gateway failed to start with `ValidationException: One or more required options are missing: address`. Required manually appending config to `config.yaml` and passing `-Dsql-gateway.endpoint.rest.address=0.0.0.0` at startup.

2. **Stale session cache** — dbt caches the session handle in `~/.dbt/flink-session.yml`. After gateway restart, the cached handle is invalid. dbt fails with `Session 'xxx' does not exist` instead of creating a new session. The error is cryptic and requires manual deletion of the cache file.

3. **No auto-restart** — The `sql-gateway` service in docker-compose had no restart policy, so a crash required manual `docker compose up -d sql-gateway`.

### Fix 1 — Add Restart Policy and Health Check to docker-compose.yml

```yaml
sql-gateway:
  image: flink-dbt-poc:1.0
  restart: unless-stopped                          # auto-restart on crash
  command: >
    bash -c "/opt/flink/bin/sql-gateway.sh start-foreground
             -Dsql-gateway.endpoint.rest.address=0.0.0.0
             -Dsql-gateway.endpoint.rest.port=8083"
  healthcheck:
    test: ["CMD", "curl", "-f", "http://localhost:8083/v3/info"]
    interval: 10s
    timeout: 5s
    retries: 5
    start_period: 30s
  ports:
    - "8083:8083"
```

### Fix 2 — Auto-Clear Stale dbt Session Before Each Run

```bash
#!/bin/bash
# scripts/dbt_run.sh — wrapper that clears stale session first

SESSION_FILE=~/.dbt/flink-session.yml

# If the session file is older than 1 hour, it's almost certainly stale
if [ -f "$SESSION_FILE" ]; then
  AGE=$(( $(date +%s) - $(stat -f %m "$SESSION_FILE" 2>/dev/null || stat -c %Y "$SESSION_FILE") ))
  if [ "$AGE" -gt 3600 ]; then
    echo "Clearing stale dbt session file (age: ${AGE}s)"
    rm -f "$SESSION_FILE"
  fi
fi

.venv39/bin/dbt run --profiles-dir dbt "$@"
```

### Fix 3 — Bake the Address Config into config.yaml

Add to `/opt/flink/conf/config.yaml` (done via Dockerfile or volume-mounted config):

```yaml
sql-gateway:
  endpoint:
    rest:
      address: 0.0.0.0
      port: 8083
```

This eliminates the need to pass `-D` flags at runtime.

---

## Gap 5 — 4 TaskManager Slots Shared by Everything

**Severity:** Medium

### What Broke

With 4 slots and 3 streaming jobs running, any batch SELECT (for verification, dbt, or
ad-hoc debugging) had 0 slots available. Batch jobs landed in `CREATED` state silently —
no error, no timeout, just blocked. We had to cancel a streaming job to run a verification
query.

Job states observed:
```
ea838d61  RUNNING   upsert streaming         (uses 1 slot)
e9f75fa9  RUNNING   cdc log streaming        (uses 1 slot)
727e158c  RUNNING   soft delete streaming    (uses 1 slot)
39e8b6dc  CREATED   batch SELECT             (blocked — 0 slots free)
```

### Fix 1 — Increase Slots per TaskManager

In `docker-compose.yml`, the TaskManager service sets slot count via env:

```yaml
taskmanager:
  environment:
    FLINK_PROPERTIES: |
      taskmanager.numberOfTaskSlots: 8    # was 4
```

This doubles the available slots at zero cost on a local machine and gives headroom for
3 streaming jobs + batch queries + dbt runs simultaneously.

### Fix 2 — Add a Second TaskManager (Better for Production)

```yaml
taskmanager-2:
  image: flink-dbt-poc:1.0
  depends_on:
    - jobmanager
  command: taskmanager
  environment:
    JOB_MANAGER_RPC_ADDRESS: jobmanager
    FLINK_PROPERTIES: |
      taskmanager.numberOfTaskSlots: 4
```

Two TaskManagers with 4 slots each = 8 total. Also provides task-level fault tolerance:
if one TM dies, the other keeps its tasks running while Flink reschedules the failed tasks.

---

## Gap 6 — No Automatic Job Recovery After Jobmanager Restart

**Severity:** Medium

### What Broke

When jobmanager was restarted (by `docker compose up`), all job history was wiped. Flink's
built-in restart strategy (`restart-strategy: fixed-delay`) handles task-level failures
within a running job, but it cannot recover jobs that were lost because the jobmanager
itself restarted.

### Fix — Enable High Availability Mode

ZooKeeper is already running in this docker-compose. Flink HA uses ZooKeeper to persist
job metadata and S3 for checkpoint/savepoint storage. When the jobmanager restarts, it
reads job metadata from ZooKeeper and automatically resumes all jobs from their last
checkpoint.

Add to `flink-conf.yaml` (or `config.yaml`):

```yaml
high-availability: zookeeper
high-availability.zookeeper.quorum: zookeeper:2181
high-availability.storageDir: s3://flink-iceberg-warehouse/flink-ha/
high-availability.zookeeper.path.root: /flink
```

With this in place, the sequence after jobmanager restart becomes:
1. Jobmanager starts, reads job list from ZooKeeper
2. Jobs are automatically resubmitted from their last checkpoint
3. Pipeline resumes with no data loss and no manual intervention

**This is the highest-leverage single change for production use.**

---

## Gap 7 — Checkpoints Are Stored in the Container (Not Durable)

**Severity:** Medium

### What Broke

Checkpointing is enabled (`EXACTLY_ONCE`, 10-second interval) but checkpoints go to the
default local filesystem inside the container. When the container is replaced, checkpoint
files are gone. EXACTLY_ONCE semantics are only guaranteed within a single container
lifetime.

### Fix — Point Checkpoints to S3

Add to `flink-conf.yaml`:

```yaml
state.checkpoints.dir: s3://flink-iceberg-warehouse/checkpoints/
state.savepoints.dir: s3://flink-iceberg-warehouse/savepoints/
state.backend: hashmap                            # or rocksdb for large state
execution.checkpointing.externalized-checkpoint-retention: RETAIN_ON_CANCELLATION
state.checkpoints.num-retained: 3                # keep last 3, auto-clean older ones
```

With `RETAIN_ON_CANCELLATION`, checkpoints persist even after a job is stopped, enabling
restart from the last known-good state with `SET 'execution.savepoint.path' = 's3://...'`.

---

## Gap 8 — No Observability

**Severity:** Medium

### What Broke

The duplicate CDC job (Gap 3) ran for an unknown duration before being discovered manually
by polling the REST API. A stalled batch query (Gap 5) had no timeout or alert. Job
failures after the jobmanager restart were discovered by noticing the pipeline was silent.

### Fix 1 — Minimum Viable Health Check (10 lines of bash)

```bash
#!/bin/bash
# scripts/health_check.sh — run as cron every 5 minutes

EXPECTED=3
RUNNING=$(curl -s http://localhost:8081/jobs | python3 -c "
import json, sys
print(sum(1 for j in json.load(sys.stdin)['jobs'] if j['status'] == 'RUNNING'))
")

if [ "$RUNNING" -lt "$EXPECTED" ]; then
  echo "ALERT: Only $RUNNING/$EXPECTED streaming jobs running at $(date)"
  # Add: curl to PagerDuty/Slack webhook here
fi

# Also check for duplicates
DUPLICATES=$(curl -s http://localhost:8081/jobs/overview | python3 -c "
import json, sys
from collections import Counter
names = [j['name'] for j in json.load(sys.stdin).get('jobs',[]) if j['status']=='RUNNING']
dups = [n for n,c in Counter(names).items() if c > 1]
print(len(dups))
")

if [ "$DUPLICATES" -gt 0 ]; then
  echo "ALERT: Duplicate streaming jobs detected at $(date)"
fi
```

Add to crontab: `*/5 * * * * /path/to/health_check.sh >> /var/log/flink-health.log 2>&1`

### Fix 2 — Prometheus Metrics (Better)

Add to `flink-conf.yaml`:

```yaml
metrics.reporters: prom
metrics.reporter.prom.class: org.apache.flink.metrics.prometheus.PrometheusReporter
metrics.reporter.prom.port: 9249
```

Key metrics to alert on:
- `flink_jobmanager_numRunningJobs < 3` → jobs are down
- `flink_taskmanager_job_task_numRecordsInPerSecond == 0` for > 5min → consumer stalled
- `flink_jobmanager_job_lastCheckpointDuration > 30000` → checkpoint taking too long
- `flink_taskmanager_Status_JVM_Memory_Heap_Used / Max > 0.85` → TM memory pressure

---

## Summary — Priority Order

| # | Gap | Change | Effort | Eliminates |
|---|-----|--------|--------|-----------|
| 1 | AWS credentials ephemeral | Mount `~/.aws` as volume | 5 min | Broken restarts due to missing creds |
| 2 | SQL Gateway crash loop | Add `restart: unless-stopped` + health check | 10 min | Manual gateway restarts |
| 3 | Stale dbt session | Wrapper script auto-clears session file | 15 min | `Session does not exist` errors |
| 4 | Slot exhaustion | Increase `numberOfTaskSlots` to 8 | 2 min | Batch queries silently blocked |
| 5 | Duplicate jobs | Name jobs + guard in submission script | 30 min | Silent data duplication |
| 6 | Checkpoint durability | Point checkpoints to S3 | 10 min | Data loss on container replace |
| 7 | No job recovery after restart | Enable Flink HA with ZooKeeper | 2 hr | Manual job resubmission after any restart |
| 8 | No observability | 10-line health check cron | 30 min | Silent failures going undetected |

Items 1–6 are all config or script changes. Together they address every failure mode
observed in these sessions without any architectural change. Item 7 (HA mode) is the
right long-term fix for production but requires more setup. Item 8 should be in place
before any of this runs unattended.
