# Flink + Iceberg Docker Debugging Summary

## Overview
This document captures the full debugging process for the Flink + Iceberg issue in the `flink_dbt_poc` repository.

The main goal was to run a Flink SQL job inside Docker that reads from Kafka and writes to Iceberg using AWS Glue and S3.

## Problems Encountered

1. `SqlClientOptions` / Flink SQL client startup failure
   - `java.lang.NoClassDefFoundError: Could not initialize class org.apache.flink.table.client.config.SqlClientOptions`
   - This occurred because the Flink SQL client jar was not available in the container classpath.

2. Docker `./lib` mount overwrote Flink runtime jars
   - Mounting `./lib:/opt/flink/lib` replaced the base Flink /opt/flink/lib directory.
   - This removed essential Flink runtime jars and caused runtime startup failures.

3. Stale or conflicting jars in `lib/`
   - Old Iceberg and Jackson artifacts were present and created classpath conflicts.
   - `commons-cli` 1.2 from Hadoop conflicted with the Flink runtime.

4. Jobmanager startup error due to `commons-cli`
   - `NoSuchMethodError: org.apache.commons.cli.Option.builder(String)`
   - This indicated an older `commons-cli` jar was being loaded instead of `commons-cli:1.4`.

5. AWS region resolution failure for Glue catalog
   - `Unable to load region from any of the providers in the chain`
   - The Flink container did not have AWS region environment variables set properly.

6. Flink SQL parser / table creation issues
   - `default` database name was used without quoting, causing parse errors.
   - Iceberg catalog table creation incorrectly used `connector='iceberg'` inside an Iceberg catalog.

## Debugging Steps Taken

### Initial inspection
- Inspected `pom.xml` for Iceberg and Flink dependencies.
- Inspected `docker-compose.yml` for service configuration, volumes, and builds.
- Inspected `scripts/run_flink_sql.sh` to see how SQL was being executed.
- Checked the contents of `lib/` for connector jar versions and duplicates.

### Dependency and Docker debugging
- Upgraded Iceberg to `1.10.1` in `pom.xml`.
- Built the Maven dependency copy using `mvn clean package`.
- Discovered the `./lib:/opt/flink/lib` mount was overwriting Flink runtime jars.
- Created `Dockerfile.flink` to bake required jars into the Flink image instead of mounting.
- Rebuilt the custom Flink image and restarted the Docker Compose services.

### Container validation
- Inspected `/opt/flink/lib` inside the `jobmanager` container.
- Verified the official Flink image contents contained no `flink-sql-client` or `flink-sql-gateway` jars by default.
- Confirmed the custom image needed the SQL client/gateway jars baked in.

### Fixing the classpath and startup issues
- Added `org.apache.flink:flink-sql-client:1.20.1` and `org.apache.flink:flink-sql-gateway:1.20.1` to `pom.xml`.
- Identified `commons-cli:1.2` transitively from `hadoop-common`.
- Added an exclusion for `commons-cli` from `hadoop-common` and added `commons-cli:1.4` explicitly.
- Rebuilt Maven dependencies and the Docker image again.
- Restarted `jobmanager`, `taskmanager`, and `sql-gateway`.

### AWS and SQL file fixes
- Added AWS environment variables to `jobmanager` and `taskmanager` in `docker-compose.yml`:
  - `AWS_REGION`
  - `AWS_DEFAULT_REGION`
  - `AWS_ACCESS_KEY_ID`
  - `AWS_SECRET_ACCESS_KEY`
  - `AWS_SESSION_TOKEN`
- Updated `sql/flink_kafka_to_iceberg.sql` to quote `default` database name:
  - `CREATE DATABASE IF NOT EXISTS `default`;`
  - `USE `default`;`
- Removed invalid Iceberg connector properties from the Iceberg table definition when using the Iceberg catalog.

## Resolution Steps Taken

### Changes made
- `pom.xml`
  - Added missing Flink SQL client and gateway jars.
  - Added `commons-cli:1.4` and excluded `commons-cli:1.2`.
- `Dockerfile.flink`
  - Copied all required jars from `lib/` into `/opt/flink/lib/`.
- `docker-compose.yml`
  - Configured `jobmanager` and `taskmanager` builds to use the custom Flink image.
  - Set AWS environment variables for Glue region resolution.
- `sql/flink_kafka_to_iceberg.sql`
  - Quoted reserved database name.
  - Adjusted Iceberg table creation to use the catalog correctly.

### Commands executed successfully
- `mvn clean package`
- `docker-compose build --no-cache jobmanager`
- `docker-compose up -d --force-recreate jobmanager taskmanager sql-gateway`
- `docker-compose exec jobmanager /opt/flink/bin/sql-client.sh -f /opt/flink/sql/flink_kafka_to_iceberg.sql`

## What Worked

- Building a custom Flink image resolved the runtime jar overwriting issue.
- Adding `flink-sql-client` and `flink-sql-gateway` fixed the `SqlClientOptions` error.
- Excluding old `commons-cli` and using version `1.4` fixed the jobmanager startup failure.
- Setting AWS region environment variables allowed Glue catalog creation to proceed.
- Fixing the SQL script syntax and Iceberg table definition allowed the SQL file to execute further.

## What Didn’t Work

- Mounting `./lib:/opt/flink/lib` directly: broke Flink runtime by replacing base jars.
- Leaving stale jars in `lib/`: caused classpath conflicts and startup crashes.
- Using unquoted `default` database name: caused SQL parse errors.
- Creating an Iceberg catalog table with `connector='iceberg'` inside an Iceberg catalog: caused Flink validation failure.

## Final State
The Flink SQL client now starts in the `jobmanager` container, and the SQL file progresses past catalog creation.

The remaining issues to verify are:
- end-to-end Kafka-to-Iceberg data flow
- any runtime connector mismatches for Iceberg / Glue specific options

---

This document is intended as a complete trace of the debugging process, including the problems encountered, the steps taken, and the final fixes applied.
