# Scalable Data Modifications — Automated Patterns

Instead of manually editing SQL files for each update/delete, here are production-ready approaches.

---

## Table of Contents

1. [Pattern 1: Requests Table + Batch Job](#pattern-1-requests-table--batch-job)
2. [Pattern 2: dbt Macros with Parameters](#pattern-2-dbt-macros-with-parameters)
3. [Pattern 3: Python Script with Configuration](#pattern-3-python-script-with-configuration)
4. [Pattern 4: Workflow Orchestration (Airflow)](#pattern-4-workflow-orchestration-airflow)
5. [Comparison & Recommendations](#comparison--recommendations)

---

## Pattern 1: Requests Table + Batch Job

### Concept
Users submit deletion/update requests to a table. A scheduled batch job processes them automatically.

```
┌─────────────────────────────────┐
│ External System (Admin Portal)   │
└────────────┬────────────────────┘
             │ INSERT into deletion_requests
             ↓
┌────────────────────────────────┐
│ deletion_requests table         │
│ - request_id                   │
│ - customer_id                  │
│ - operation (DELETE, MASK_PII) │
│ - status (pending, completed)  │
└────────┬───────────────────────┘
         │
         ↓ (daily batch job)
┌────────────────────────────────┐
│ Flink Batch Job                │
│ Reads requests → applies mods   │
│ Updates request status         │
└────────────────────────────────┘
         │
         ↓
┌────────────────────────────────┐
│ iceberg_orders (modified)      │
└────────────────────────────────┘
```

### Step 1: Create Requests Table

```sql
SET 'execution.runtime-mode' = 'batch';

CREATE CATALOG IF NOT EXISTS iceberg_catalog WITH (
  'type' = 'iceberg',
  'warehouse' = 's3://flink-iceberg-warehouse/',
  'catalog-impl' = 'org.apache.iceberg.aws.glue.GlueCatalog',
  'io-impl' = 'org.apache.iceberg.aws.s3.S3FileIO'
);

USE CATALOG iceberg_catalog;
USE `default`;

-- Track all modification requests
CREATE TABLE IF NOT EXISTS data_modification_requests (
  request_id STRING,
  operation STRING,              -- DELETE, UPDATE_STATUS, MASK_PII
  target_table STRING,           -- iceberg_orders
  filter_condition STRING,       -- customer_id = 'cust_1001'
  update_values STRING,          -- status = 'verified' (for UPDATE)
  requested_by STRING,
  requested_at TIMESTAMP(3),
  status STRING,                 -- pending, processing, completed, failed
  error_message STRING,
  processed_at TIMESTAMP(3),
  affected_rows BIGINT
);
```

### Step 2: Submit Requests via INSERT

```sql
-- Request 1: Delete customer data (GDPR)
INSERT INTO data_modification_requests VALUES (
  'REQ-20260501-001',
  'DELETE',
  'iceberg_orders',
  "customer_id = 'cust_1001'",
  NULL,
  'compliance@company.com',
  CURRENT_TIMESTAMP,
  'pending',
  NULL,
  NULL,
  NULL
);

-- Request 2: Mark orders as verified
INSERT INTO data_modification_requests VALUES (
  'REQ-20260501-002',
  'UPDATE_STATUS',
  'iceberg_orders',
  "order_ts >= TIMESTAMP '2026-04-01' AND order_ts < TIMESTAMP '2026-05-01'",
  "status = 'verified'",
  'data-team@company.com',
  CURRENT_TIMESTAMP,
  'pending',
  NULL,
  NULL,
  NULL
);

-- Request 3: Mask PII for old orders
INSERT INTO data_modification_requests VALUES (
  'REQ-20260501-003',
  'MASK_PII',
  'iceberg_orders',
  "order_ts < TIMESTAMP '2025-01-01'",
  "customer_id = CONCAT('MASKED_', MD5(customer_id))",
  'privacy-officer@company.com',
  CURRENT_TIMESTAMP,
  'pending',
  NULL,
  NULL,
  NULL
);
```

### Step 3: Batch Job (Process Requests)

Create `sql/process_data_modifications.sql`:

```sql
SET 'execution.runtime-mode' = 'batch';

CREATE CATALOG IF NOT EXISTS iceberg_catalog WITH (
  'type' = 'iceberg',
  'warehouse' = 's3://flink-iceberg-warehouse/',
  'catalog-impl' = 'org.apache.iceberg.aws.glue.GlueCatalog',
  'io-impl' = 'org.apache.iceberg.aws.s3.S3FileIO'
);

USE CATALOG iceberg_catalog;
USE `default`;

-- Process each pending request
-- This is pseudo-code: actual implementation requires dynamic SQL which Flink doesn't support
-- See Python script (Pattern 3) for real implementation

-- Step 1: Find pending requests
SELECT request_id, operation, target_table, filter_condition, update_values
FROM data_modification_requests
WHERE status = 'pending'
LIMIT 1;

-- Step 2: For DELETE requests
-- DELETE FROM iceberg_orders WHERE <filter_condition>

-- Step 3: For UPDATE requests
-- UPDATE iceberg_orders SET <update_values> WHERE <filter_condition>

-- Step 4: Update request status
-- UPDATE data_modification_requests SET status = 'completed', processed_at = CURRENT_TIMESTAMP
```

### Step 4: Python Script (Real Implementation)

See Pattern 3 below — Python can iterate over requests and execute dynamic SQL.

### Advantages
✅ Fully automated  
✅ Audit trail of all requests  
✅ No SQL editing required  
✅ Easy to integrate with external systems  

### Disadvantages
❌ Requires Python/external script  
❌ Slightly more complex setup  

---

## Pattern 2: dbt Macros with Parameters

### Concept
Create reusable dbt macros that accept parameters. dbt CLI passes arguments.

### Step 1: Create dbt Macro

Create `dbt/macros/apply_data_modification.sql`:

```sql
{% macro apply_data_modification(operation, target_table, filter_condition, update_values) %}
  {% if execute %}
    {% set sql %}
    SET 'execution.runtime-mode' = 'batch';

    CREATE CATALOG IF NOT EXISTS iceberg_catalog WITH (
      'type' = 'iceberg',
      'warehouse' = 's3://flink-iceberg-warehouse/',
      'catalog-impl' = 'org.apache.iceberg.aws.glue.GlueCatalog',
      'io-impl' = 'org.apache.iceberg.aws.s3.S3FileIO'
    );

    USE CATALOG iceberg_catalog;
    USE `default`;

    {% if operation == 'DELETE' %}
      DELETE FROM {{ target_table }}
      WHERE {{ filter_condition }};
    
    {% elif operation == 'UPDATE' %}
      UPDATE {{ target_table }}
      SET {{ update_values }}
      WHERE {{ filter_condition }};
    
    {% elif operation == 'MASK_PII' %}
      UPDATE {{ target_table }}
      SET {{ update_values }}
      WHERE {{ filter_condition }};
    
    {% else %}
      {{ exceptions.raise_compiler_error('Unknown operation: ' ~ operation) }}
    {% endif %}

    -- Log the operation
    {{ log(operation ~ ' completed on ' ~ target_table, info=True) }}
    {% endset %}

    {% set result = run_query(sql) %}
  {% endif %}
{% endmacro %}
```

### Step 2: Create a Wrapper Model

Create `dbt/models/run_modification.sql`:

```sql
-- This model doesn't create a table, just runs a macro

{% if execute %}
  -- Extract parameters from flags or variables
  {% set operation = var('modification_operation', 'DELETE') %}
  {% set target_table = var('modification_target', 'iceberg_orders') %}
  {% set filter = var('modification_filter', '1=0') %}  -- Default: no-op
  {% set update_vals = var('modification_update', '') %}

  {{ apply_data_modification(operation, target_table, filter, update_vals) }}
  
  SELECT 'Modification applied' as result
{% else %}
  SELECT 'Dry run' as result
{% endif %}
```

### Step 3: Run with Parameters

```bash
# Delete customer data
.venv39/bin/dbt run --profiles-dir dbt \
  --select run_modification \
  --vars '{
    modification_operation: DELETE,
    modification_target: iceberg_orders,
    modification_filter: "customer_id = '\''cust_1001'\''"
  }'

# Update statuses
.venv39/bin/dbt run --profiles-dir dbt \
  --select run_modification \
  --vars '{
    modification_operation: UPDATE,
    modification_target: iceberg_orders,
    modification_filter: "order_ts >= TIMESTAMP '\''2026-04-01'\'' AND order_ts < TIMESTAMP '\''2026-05-01'\''",
    modification_update: "status = '\''verified'\''"
  }'

# Mask PII
.venv39/bin/dbt run --profiles-dir dbt \
  --select run_modification \
  --vars '{
    modification_operation: MASK_PII,
    modification_target: iceberg_orders,
    modification_filter: "order_ts < TIMESTAMP '\''2025-01-01'\''",
    modification_update: "customer_id = CONCAT('\''MASKED_'\'', MD5(customer_id))"
  }'
```

### Advantages
✅ No SQL file editing  
✅ Version controlled (git)  
✅ Integrates with dbt workflows  
✅ Can add tests and documentation  

### Disadvantages
❌ Complex quoting in CLI  
❌ Limited to dbt model runs  

---

## Pattern 3: Python Script with Configuration

### Concept
Python script reads from a config file or table, executes modifications dynamically.

Create `scripts/apply_data_modifications.py`:

```python
#!/usr/bin/env python3
"""
Apply data modifications based on a configuration file or database table.

Usage:
    python3 scripts/apply_data_modifications.py --config modifications.yaml
    python3 scripts/apply_data_modifications.py --from-table pending
"""

import json
import argparse
import urllib.request
import time
import yaml
import sys
from datetime import datetime

class FlinkModificationManager:
    def __init__(self, gateway_url="http://localhost:8083"):
        self.gateway_url = gateway_url
        self.session_id = None

    def create_session(self):
        """Create a new SQL Gateway session."""
        url = f"{self.gateway_url}/v1/sessions"
        data = {"sessionName": f"modification_{int(time.time())}"}
        
        req = urllib.request.Request(
            url,
            json.dumps(data).encode(),
            {"Content-Type": "application/json"}
        )
        with urllib.request.urlopen(req) as r:
            result = json.loads(r.read())
            self.session_id = result["sessionHandle"]
            print(f"✓ Created session: {self.session_id}")

    def execute_sql(self, sql_statement, label=""):
        """Execute a SQL statement and return operation handle."""
        url = f"{self.gateway_url}/v1/sessions/{self.session_id}/statements"
        data = {
            "statement": sql_statement,
            "executionConfig": {"execution.runtime-mode": "batch"}
        }
        
        req = urllib.request.Request(
            url,
            json.dumps(data).encode(),
            {"Content-Type": "application/json"}
        )
        with urllib.request.urlopen(req) as r:
            result = json.loads(r.read())
            op_id = result["operationHandle"]
            print(f"  Submitted: {label or sql_statement[:50]}")
            return op_id

    def wait_for_completion(self, op_id, timeout=300):
        """Poll until operation completes."""
        url = f"{self.gateway_url}/v1/sessions/{self.session_id}/operations/{op_id}/status"
        start_time = time.time()
        
        while time.time() - start_time < timeout:
            with urllib.request.urlopen(url) as r:
                result = json.loads(r.read())
                status = result["status"]
                
                if status == "FINISHED":
                    return True
                elif status in ("ERROR", "CANCELED"):
                    return False
                
                time.sleep(2)
        
        return False

    def apply_modification(self, operation, target_table, filter_condition, update_values=None):
        """Apply a single modification (DELETE or UPDATE)."""
        if operation == "DELETE":
            sql = f"DELETE FROM {target_table} WHERE {filter_condition};"
            label = f"DELETE {target_table} ({filter_condition[:30]}...)"
        
        elif operation == "UPDATE":
            sql = f"UPDATE {target_table} SET {update_values} WHERE {filter_condition};"
            label = f"UPDATE {target_table} ({filter_condition[:30]}...)"
        
        else:
            raise ValueError(f"Unknown operation: {operation}")
        
        op_id = self.execute_sql(sql, label)
        success = self.wait_for_completion(op_id)
        
        if success:
            print(f"    ✓ {operation} completed")
            return True
        else:
            print(f"    ✗ {operation} failed")
            return False

    def apply_catalog_setup(self):
        """Register Iceberg catalog."""
        sql = """
        CREATE CATALOG IF NOT EXISTS iceberg_catalog WITH (
          'type'='iceberg',
          'warehouse'='s3://flink-iceberg-warehouse/',
          'catalog-impl'='org.apache.iceberg.aws.glue.GlueCatalog',
          'io-impl'='org.apache.iceberg.aws.s3.S3FileIO'
        );
        """
        op_id = self.execute_sql(sql, "Register Iceberg catalog")
        return self.wait_for_completion(op_id)

def load_config_yaml(filename):
    """Load modifications from YAML file."""
    with open(filename, 'r') as f:
        return yaml.safe_load(f)

def load_from_database(manager):
    """Load modifications from data_modification_requests table (not implemented)."""
    # This would require querying the table and parsing results
    # For now, users can export to YAML first
    raise NotImplementedError("Use YAML config for now. TODO: implement direct table query")

def main():
    parser = argparse.ArgumentParser(description="Apply data modifications")
    parser.add_argument("--config", help="YAML config file with modifications")
    parser.add_argument("--from-table", help="Load from data_modification_requests table")
    parser.add_argument("--gateway", default="http://localhost:8083", help="Flink SQL Gateway URL")
    parser.add_argument("--dry-run", action="store_true", help="Don't actually execute")
    
    args = parser.parse_args()
    
    if not args.config and not args.from_table:
        parser.print_help()
        return 1
    
    # Load configuration
    if args.config:
        config = load_config_yaml(args.config)
    else:
        config = load_from_database(FlinkModificationManager(args.gateway))
    
    if args.dry_run:
        print("DRY RUN MODE - No changes will be applied\n")
    
    manager = FlinkModificationManager(args.gateway)
    
    try:
        manager.create_session()
        manager.apply_catalog_setup()
        
        results = {"success": 0, "failed": 0}
        
        for i, mod in enumerate(config.get("modifications", []), 1):
            print(f"\n[{i}/{len(config['modifications'])}] {mod['name']}")
            
            if args.dry_run:
                print(f"  Would execute: {mod['operation']} on {mod['target_table']}")
                results["success"] += 1
            else:
                success = manager.apply_modification(
                    operation=mod["operation"],
                    target_table=mod["target_table"],
                    filter_condition=mod["filter_condition"],
                    update_values=mod.get("update_values")
                )
                results["success" if success else "failed"] += 1
        
        print(f"\n{'='*50}")
        print(f"Results: {results['success']} succeeded, {results['failed']} failed")
        
        return 0 if results["failed"] == 0 else 1
    
    except Exception as e:
        print(f"Error: {e}", file=sys.stderr)
        return 1

if __name__ == "__main__":
    sys.exit(main())
```

### Step 4: Create YAML Config File

Create `modifications.yaml`:

```yaml
modifications:
  - name: "GDPR: Delete customer cust_1001"
    operation: DELETE
    target_table: iceberg_catalog.`default`.iceberg_orders
    filter_condition: "customer_id = 'cust_1001'"

  - name: "Archive: Verify April orders"
    operation: UPDATE
    target_table: iceberg_catalog.`default`.iceberg_orders
    filter_condition: "order_ts >= TIMESTAMP '2026-04-01' AND order_ts < TIMESTAMP '2026-05-01'"
    update_values: "status = 'verified'"

  - name: "Privacy: Mask old customer IDs"
    operation: UPDATE
    target_table: iceberg_catalog.`default`.iceberg_orders
    filter_condition: "order_ts < TIMESTAMP '2025-01-01'"
    update_values: "customer_id = CONCAT('MASKED_', MD5(customer_id))"
```

### Step 5: Run It

```bash
# Install dependencies
pip install pyyaml

# Dry run (preview changes)
python3 scripts/apply_data_modifications.py --config modifications.yaml --dry-run

# Execute
python3 scripts/apply_data_modifications.py --config modifications.yaml

# Track results
python3 scripts/apply_data_modifications.py --config modifications.yaml 2>&1 | tee modification_$(date +%Y%m%d_%H%M%S).log
```

### Output Example
```
✓ Created session: abc123
  Submitted: Register Iceberg catalog
    ✓ CREATE CATALOG completed

[1/3] GDPR: Delete customer cust_1001
  Submitted: DELETE iceberg_catalog.`default`.iceberg_orders...
    ✓ DELETE completed

[2/3] Archive: Verify April orders
  Submitted: UPDATE iceberg_catalog.`default`.iceberg_orders...
    ✓ UPDATE completed

[3/3] Privacy: Mask old customer IDs
  Submitted: UPDATE iceberg_catalog.`default`.iceberg_orders...
    ✓ UPDATE completed

==================================================
Results: 3 succeeded, 0 failed
```

### Advantages
✅ Fully dynamic (no code changes)  
✅ Easy to audit (YAML is human-readable)  
✅ Integrates with CI/CD pipelines  
✅ Can read from database table  
✅ Logging and error handling built-in  

### Disadvantages
❌ Requires Python environment  
❌ One more tool to manage  

---

## Pattern 4: Workflow Orchestration (Airflow)

### Concept
Use Apache Airflow (or similar) to schedule and monitor modifications.

Create `dags/data_modifications_dag.py`:

```python
from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import json
import urllib.request

default_args = {
    'owner': 'data-engineering',
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

def execute_modification(operation, target_table, filter_condition, update_values=None):
    """Execute a single modification."""
    session_url = "http://localhost:8083/v1/sessions"
    session_data = {"sessionName": f"airflow_task_{datetime.now().timestamp()}"}
    
    req = urllib.request.Request(
        session_url,
        json.dumps(session_data).encode(),
        {"Content-Type": "application/json"}
    )
    with urllib.request.urlopen(req) as r:
        session_id = json.loads(r.read())["sessionHandle"]
    
    # Build SQL
    if operation == "DELETE":
        sql = f"DELETE FROM {target_table} WHERE {filter_condition};"
    else:
        sql = f"UPDATE {target_table} SET {update_values} WHERE {filter_condition};"
    
    # Execute
    statement_url = f"{session_url.rsplit('/', 1)[0]}/sessions/{session_id}/statements"
    stmt_data = {"statement": sql, "executionConfig": {"execution.runtime-mode": "batch"}}
    
    req = urllib.request.Request(
        statement_url,
        json.dumps(stmt_data).encode(),
        {"Content-Type": "application/json"}
    )
    with urllib.request.urlopen(req) as r:
        print(f"✓ {operation} executed on {target_table}")

with DAG(
    'data_modifications_daily',
    default_args=default_args,
    schedule_interval='0 2 * * *',  # 2 AM daily
    catchup=False,
) as dag:
    
    # Scheduled job: Delete old test data monthly
    delete_test_data = PythonOperator(
        task_id='delete_test_data',
        python_callable=execute_modification,
        op_kwargs={
            'operation': 'DELETE',
            'target_table': 'iceberg_catalog.`default`.iceberg_orders',
            'filter_condition': "customer_id LIKE 'test_%'",
        }
    )
    
    # Scheduled job: Mask PII yearly
    mask_pii_yearly = PythonOperator(
        task_id='mask_pii_yearly',
        python_callable=execute_modification,
        op_kwargs={
            'operation': 'UPDATE',
            'target_table': 'iceberg_catalog.`default`.iceberg_orders',
            'filter_condition': f"order_ts < TIMESTAMP '{(datetime.now() - timedelta(days=365)).date()}'",
            'update_values': "customer_id = CONCAT('MASKED_', MD5(customer_id))",
        }
    )
    
    delete_test_data >> mask_pii_yearly
```

### Advantages
✅ Scheduled execution (recurring jobs)  
✅ Dependency management  
✅ Monitoring and alerting  
✅ Easy to trigger ad-hoc  

### Disadvantages
❌ Requires Airflow infrastructure  
❌ More complex setup  

---

## Comparison & Recommendations

| Pattern | Setup Effort | Scalability | Best For |
|---------|--------------|-------------|----------|
| **Requests Table** | Medium | ⭐⭐⭐⭐⭐ | Ad-hoc + recurring requests |
| **dbt Macros** | Low | ⭐⭐⭐ | dbt-based workflows |
| **Python Script** | Medium | ⭐⭐⭐⭐ | On-demand, CI/CD pipelines |
| **Airflow DAG** | High | ⭐⭐⭐⭐⭐ | Scheduled recurring jobs |

### My Recommendation

**Start with Pattern 3 (Python Script)**
- Covers 80% of use cases
- Easy to set up (single script)
- Works immediately
- Can evolve to Airflow later

**Add Pattern 1 (Requests Table) when:**
- Need audit trail of all requests
- Multiple teams submit requests
- GDPR compliance required

**Graduate to Pattern 4 (Airflow) when:**
- Running 10+ recurring modifications
- Need scheduling, monitoring, alerting
- Already using Airflow in organization

---

## Quick Start

```bash
# 1. Create requests table (one-time)
docker exec flink_dbt_poc-jobmanager-1 \
  /opt/flink/bin/sql-client.sh << 'EOF'
SET 'execution.runtime-mode' = 'batch';
CREATE CATALOG IF NOT EXISTS iceberg_catalog WITH (...);
USE CATALOG iceberg_catalog;
USE `default`;

CREATE TABLE IF NOT EXISTS data_modification_requests (
  request_id STRING,
  operation STRING,
  target_table STRING,
  filter_condition STRING,
  update_values STRING,
  requested_by STRING,
  requested_at TIMESTAMP(3),
  status STRING,
  error_message STRING,
  processed_at TIMESTAMP(3),
  affected_rows BIGINT
);
EOF

# 2. Create Python script (see Pattern 3)

# 3. Create YAML config with your modifications

# 4. Run on-demand
python3 scripts/apply_data_modifications.py --config modifications.yaml

# 5. Or schedule with cron
echo "0 2 * * * python3 /path/to/scripts/apply_data_modifications.py --config /path/to/modifications.yaml" | crontab -
```

Done! Now you have fully automated, scalable data modifications without touching SQL files.
