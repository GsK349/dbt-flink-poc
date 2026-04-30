#!/usr/bin/env bash
set -euo pipefail

# AWS Setup script for Glue Catalog with Flink Iceberg POC
# This script configures AWS credentials and creates necessary S3 resources

echo "=== AWS Glue Catalog Setup ==="

# Check if AWS CLI is installed
if ! command -v aws &> /dev/null; then
    echo "ERROR: AWS CLI not found. Please install it first: https://aws.amazon.com/cli/"
    exit 1
fi

# Prompt for AWS configuration
echo ""
echo "Please enter your AWS credentials:"
read -p "AWS Access Key ID: " AWS_ACCESS_KEY_ID
read -sp "AWS Secret Access Key: " AWS_SECRET_ACCESS_KEY
echo ""
read -p "AWS Region (default: us-east-1): " AWS_REGION
AWS_REGION=${AWS_REGION:-us-east-1}

# Create S3 bucket for Iceberg warehouse
BUCKET_NAME="flink-iceberg-warehouse-$(date +%s)"
echo ""
echo "Creating S3 bucket: $BUCKET_NAME in region $AWS_REGION"

aws s3 mb "s3://$BUCKET_NAME" --region "$AWS_REGION" || true

# Save credentials to .env file
cat > .env.aws << EOF
AWS_REGION=$AWS_REGION
AWS_ACCESS_KEY_ID=$AWS_ACCESS_KEY_ID
AWS_SECRET_ACCESS_KEY=$AWS_SECRET_ACCESS_KEY
S3_BUCKET=$BUCKET_NAME
EOF

echo ""
echo "✓ AWS setup complete!"
echo "✓ S3 bucket created: $BUCKET_NAME"
echo "✓ Credentials saved to .env.aws"
echo ""
echo "Next steps:"
echo "1. Update sql/flink_kafka_to_iceberg.sql to use: s3://$BUCKET_NAME/"
echo "2. Run: docker-compose --env-file .env.aws up -d"
echo "3. Run: ./scripts/run_flink_sql.sh"
