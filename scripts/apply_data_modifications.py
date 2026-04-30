#!/usr/bin/env python3
"""
Apply data modifications (UPDATE/DELETE) based on YAML configuration.

This script reads a YAML file defining modifications and applies them to Iceberg
via the Flink SQL Gateway REST API. No manual SQL editing required.

Usage:
    python3 scripts/apply_data_modifications.py --config modifications.yaml
    python3 scripts/apply_data_modifications.py --config modifications.yaml --dry-run
    python3 scripts/apply_data_modifications.py --config modifications.yaml --gateway http://flink:8083

Example YAML config (modifications.yaml):
    modifications:
      - name: "Delete customer cust_1001"
        operation: DELETE
        target_table: iceberg_catalog.`default`.iceberg_orders
        filter_condition: "customer_id = 'cust_1001'"

      - name: "Update April orders status"
        operation: UPDATE
        target_table: iceberg_catalog.`default`.iceberg_orders
        filter_condition: "order_ts >= TIMESTAMP '2026-04-01' AND order_ts < TIMESTAMP '2026-05-01'"
        update_values: "status = 'verified'"
"""

import json
import argparse
import urllib.request
import time
import yaml
import sys
from datetime import datetime
from typing import Optional, Dict, List, Tuple


class FlinkModificationManager:
    """Manages data modifications via Flink SQL Gateway."""

    def __init__(self, gateway_url: str = "http://localhost:8083"):
        self.gateway_url = gateway_url.rstrip("/")
        self.session_id = None
        self.operations_completed = 0
        self.operations_failed = 0

    def create_session(self) -> bool:
        """Create a new SQL Gateway session."""
        try:
            url = f"{self.gateway_url}/v1/sessions"
            data = {"sessionName": f"modification_{int(time.time())}"}

            req = urllib.request.Request(
                url,
                json.dumps(data).encode(),
                {"Content-Type": "application/json"},
            )
            with urllib.request.urlopen(req) as r:
                result = json.loads(r.read())
                self.session_id = result["sessionHandle"]
                print(f"✓ Created session: {self.session_id}")
                return True
        except Exception as e:
            print(f"✗ Failed to create session: {e}", file=sys.stderr)
            return False

    def execute_sql(self, sql_statement: str, label: str = "") -> Optional[str]:
        """Execute a SQL statement and return operation handle."""
        try:
            url = f"{self.gateway_url}/v1/sessions/{self.session_id}/statements"
            data = {
                "statement": sql_statement,
                "executionConfig": {"execution.runtime-mode": "batch"},
            }

            req = urllib.request.Request(
                url,
                json.dumps(data).encode(),
                {"Content-Type": "application/json"},
            )
            with urllib.request.urlopen(req) as r:
                result = json.loads(r.read())
                op_id = result["operationHandle"]
                display_label = label or sql_statement[:50]
                print(f"  Submitted: {display_label}")
                return op_id
        except Exception as e:
            print(f"  ✗ Failed to submit: {e}", file=sys.stderr)
            return None

    def wait_for_completion(self, op_id: str, timeout: int = 300) -> bool:
        """Poll until operation completes."""
        try:
            url = f"{self.gateway_url}/v1/sessions/{self.session_id}/operations/{op_id}/status"
            start_time = time.time()

            while time.time() - start_time < timeout:
                with urllib.request.urlopen(url) as r:
                    result = json.loads(r.read())
                    status = result["status"]

                    if status == "FINISHED":
                        return True
                    elif status in ("ERROR", "CANCELED"):
                        print(f"    ✗ Operation {status}", file=sys.stderr)
                        return False

                    time.sleep(2)

            print(f"    ✗ Operation timed out", file=sys.stderr)
            return False
        except Exception as e:
            print(f"    ✗ Error polling status: {e}", file=sys.stderr)
            return False

    def apply_catalog_setup(self) -> bool:
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
        if not op_id:
            return False
        success = self.wait_for_completion(op_id)
        if success:
            print("    ✓ Catalog registered")
        return success

    def apply_modification(
        self,
        operation: str,
        target_table: str,
        filter_condition: str,
        update_values: Optional[str] = None,
        dry_run: bool = False,
    ) -> bool:
        """Apply a single modification (DELETE or UPDATE)."""
        if operation == "DELETE":
            sql = f"DELETE FROM {target_table} WHERE {filter_condition};"
            label = f"DELETE {target_table} WHERE {filter_condition[:30]}..."
        elif operation == "UPDATE":
            sql = f"UPDATE {target_table} SET {update_values} WHERE {filter_condition};"
            label = f"UPDATE {target_table} SET {update_values[:20]}... WHERE {filter_condition[:20]}..."
        else:
            print(f"    ✗ Unknown operation: {operation}", file=sys.stderr)
            return False

        if dry_run:
            print(f"    [DRY RUN] Would execute: {sql}")
            return True

        op_id = self.execute_sql(sql, label)
        if not op_id:
            return False

        success = self.wait_for_completion(op_id)
        if success:
            print(f"    ✓ {operation} completed")
        return success

    def get_summary(self) -> str:
        """Get summary of operations."""
        total = self.operations_completed + self.operations_failed
        return (
            f"\n{'='*60}\n"
            f"Results: {self.operations_completed}/{total} succeeded\n"
            f"{'='*60}"
        )


def load_config_yaml(filename: str) -> Optional[Dict]:
    """Load modifications from YAML file."""
    try:
        with open(filename, "r") as f:
            return yaml.safe_load(f)
    except FileNotFoundError:
        print(f"✗ Config file not found: {filename}", file=sys.stderr)
        return None
    except yaml.YAMLError as e:
        print(f"✗ YAML parse error: {e}", file=sys.stderr)
        return None


def validate_config(config: Dict) -> Tuple[bool, str]:
    """Validate configuration structure."""
    if not config:
        return False, "Empty configuration"

    if "modifications" not in config:
        return False, "Missing 'modifications' key in config"

    if not isinstance(config["modifications"], list):
        return False, "'modifications' must be a list"

    for i, mod in enumerate(config["modifications"]):
        if "operation" not in mod:
            return False, f"Modification {i} missing 'operation'"
        if "target_table" not in mod:
            return False, f"Modification {i} missing 'target_table'"
        if "filter_condition" not in mod:
            return False, f"Modification {i} missing 'filter_condition'"
        if mod["operation"] == "UPDATE" and "update_values" not in mod:
            return False, f"Modification {i} (UPDATE) missing 'update_values'"

    return True, "Valid configuration"


def main():
    parser = argparse.ArgumentParser(
        description="Apply data modifications (UPDATE/DELETE) to Iceberg via Flink SQL Gateway",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  python3 scripts/apply_data_modifications.py --config modifications.yaml
  python3 scripts/apply_data_modifications.py --config modifications.yaml --dry-run
  python3 scripts/apply_data_modifications.py --config modifications.yaml --gateway http://flink:8083
        """,
    )
    parser.add_argument(
        "--config", required=True, help="YAML configuration file with modifications"
    )
    parser.add_argument(
        "--gateway",
        default="http://localhost:8083",
        help="Flink SQL Gateway URL (default: http://localhost:8083)",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Preview modifications without applying them",
    )

    args = parser.parse_args()

    # Load and validate configuration
    config = load_config_yaml(args.config)
    if not config:
        return 1

    is_valid, message = validate_config(config)
    if not is_valid:
        print(f"✗ Configuration error: {message}", file=sys.stderr)
        return 1

    print(f"Configuration: {args.config}")
    print(f"Gateway: {args.gateway}")
    if args.dry_run:
        print("Mode: DRY RUN (no changes will be applied)")
    print()

    # Initialize manager and create session
    manager = FlinkModificationManager(args.gateway)

    if not manager.create_session():
        return 1

    if not args.dry_run:
        if not manager.apply_catalog_setup():
            return 1

    # Apply each modification
    modifications = config.get("modifications", [])
    for i, mod in enumerate(modifications, 1):
        mod_name = mod.get("name", f"Modification {i}")
        print(f"\n[{i}/{len(modifications)}] {mod_name}")

        success = manager.apply_modification(
            operation=mod["operation"],
            target_table=mod["target_table"],
            filter_condition=mod["filter_condition"],
            update_values=mod.get("update_values"),
            dry_run=args.dry_run,
        )

        if success:
            manager.operations_completed += 1
        else:
            manager.operations_failed += 1

    # Summary
    print(manager.get_summary())

    if args.dry_run:
        print("\n✓ Dry run completed. Run without --dry-run to apply changes.")

    return 0 if manager.operations_failed == 0 else 1


if __name__ == "__main__":
    sys.exit(main())
