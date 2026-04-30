#!/usr/bin/env bash
set -euo pipefail

# Flink Iceberg + AWS Glue Setup Script
# Sets up AWS credentials and creates S3 warehouse bucket

echo "=== AWS Glue + Iceberg Setup for Flink ==="
echo ""

# Check if AWS CLI is installed
if ! command -v aws &> /dev/null; then
    echo "ERROR: AWS CLI not found. Install from https://aws.amazon.com/cli/"
    exit 1
fi

# Check if .env.aws exists
if [ ! -f .env.aws ]; then
    echo "ERROR: .env.aws not found. Please create it from .env.example:"
    echo "  cp .env.example .env.aws"
    echo "  Edit .env.aws with your AWS credentials"
    exit 1
fi

# Load AWS credentials from .env.aws
set -a
source .env.aws
set +a

echo "✓ AWS credentials loaded from .env.aws"
echo "  Region: $AWS_REGION"
echo "  Access Key: ${AWS_ACCESS_KEY_ID:0:10}..."
echo ""

# Test AWS credentials
echo "Testing AWS credentials..."
if ! aws sts get-caller-identity --region "$AWS_REGION" > /dev/null 2>&1; then
    echo "ERROR: AWS credentials invalid or not configured properly"
    exit 1
fi
echo "✓ AWS credentials validated"
echo ""

# Create S3 bucket if it doesn't exist
BUCKET_NAME="${S3_BUCKET:-flink-iceberg-warehouse}"
echo "Checking S3 bucket: $BUCKET_NAME"

if aws s3 ls "s3://$BUCKET_NAME" --region "$AWS_REGION" > /dev/null 2>&1; then
    echo "✓ Bucket already exists: $BUCKET_NAME"
else
    echo "Creating new bucket: $BUCKET_NAME"
    if [ "$AWS_REGION" = "us-east-1" ]; then
        aws s3 mb "s3://$BUCKET_NAME" || true
    else
        aws s3 mb "s3://$BUCKET_NAME" --region "$AWS_REGION" || true
    fi
    echo "✓ Bucket created/verified: $BUCKET_NAME"
fi

echo ""
echo "=== Setup Complete ==="
echo ""
echo "Next steps:"
echo "1. Start Docker with AWS credentials:"
echo "   docker-compose --env-file .env.aws up -d"
echo ""
echo "2. Create Kafka topic and publish sample data:"
echo "   ./scripts/create_kafka_topic.sh"
echo "   ./scripts/produce_sample_orders.sh"
echo ""
echo "3. Run the Flink SQL job (writes to Glue + S3):"
echo "   ./scripts/run_flink_sql.sh"
echo ""
