#!/usr/bin/env bash
# Deploy or update the MSAF (Amazon Managed Service for Apache Flink) application.
# Usage: ./scripts/deploy_msaf_job.sh [--sql-only]
#
# Expects environment variables:
#   TEAM_NAME         - e.g. team-alpha
#   AWS_REGION        - e.g. us-east-1
#   APP_VERSION       - artifact version tag, e.g. v1.2.0
#   SQL_FILE          - path to local SQL file (default: sql/flink_kafka_to_iceberg.sql)

set -euo pipefail

TEAM_NAME="${TEAM_NAME:?TEAM_NAME is required}"
AWS_REGION="${AWS_REGION:-us-east-1}"
APP_VERSION="${APP_VERSION:-latest}"
SQL_FILE="${SQL_FILE:-sql/flink_kafka_to_iceberg.sql}"
ARTIFACTS_BUCKET="${TEAM_NAME}-flink-artifacts"
APP_NAME="${TEAM_NAME}-flink-kafka-iceberg"

# Upload artifacts to S3
JAR_KEY="${APP_VERSION}/flink-job-all.jar"
SQL_KEY="${APP_VERSION}/$(basename "$SQL_FILE")"

echo "==> Uploading artifacts to s3://${ARTIFACTS_BUCKET}/${APP_VERSION}/"

if [[ -f "target/flink-job-all.jar" ]]; then
  aws s3 cp target/flink-job-all.jar "s3://${ARTIFACTS_BUCKET}/${JAR_KEY}" \
    --region "${AWS_REGION}"
  echo "    JAR uploaded: ${JAR_KEY}"
fi

aws s3 cp "${SQL_FILE}" "s3://${ARTIFACTS_BUCKET}/${SQL_KEY}" \
  --region "${AWS_REGION}"
echo "    SQL uploaded: ${SQL_KEY}"

# Get current application version ID
APP_VERSION_ID=$(aws kinesisanalyticsv2 describe-application \
  --application-name "${APP_NAME}" \
  --region "${AWS_REGION}" \
  --query "ApplicationDetail.ApplicationVersionId" \
  --output text 2>/dev/null || echo "")

if [[ -z "${APP_VERSION_ID}" ]]; then
  echo "==> Application ${APP_NAME} not found. Run 'terraform apply' first to create it."
  exit 1
fi

echo "==> Updating MSAF application ${APP_NAME} (version ${APP_VERSION_ID})..."

aws kinesisanalyticsv2 update-application \
  --application-name "${APP_NAME}" \
  --current-application-version-id "${APP_VERSION_ID}" \
  --region "${AWS_REGION}" \
  --application-configuration-update "{
    \"ApplicationCodeConfigurationUpdate\": {
      \"CodeContentUpdate\": {
        \"S3ContentLocationUpdate\": {
          \"BucketARNUpdate\": \"arn:aws:s3:::${ARTIFACTS_BUCKET}\",
          \"FileKeyUpdate\": \"${JAR_KEY}\"
        }
      }
    }
  }"

echo "==> Starting application..."
aws kinesisanalyticsv2 start-application \
  --application-name "${APP_NAME}" \
  --region "${AWS_REGION}" \
  --run-configuration '{}' 2>/dev/null || echo "    Application may already be running."

echo "==> Done. Monitor at:"
echo "    https://${AWS_REGION}.console.aws.amazon.com/kinesisanalytics/home?region=${AWS_REGION}#/applications/${APP_NAME}/dashboard"
