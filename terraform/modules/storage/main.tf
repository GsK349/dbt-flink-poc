resource "aws_s3_bucket" "warehouse" {
  bucket        = var.s3_warehouse_bucket
  force_destroy = false
  tags          = { Name = var.s3_warehouse_bucket, Team = var.team_name }
}

resource "aws_s3_bucket_versioning" "warehouse" {
  bucket = aws_s3_bucket.warehouse.id
  versioning_configuration { status = "Enabled" }
}

resource "aws_s3_bucket_server_side_encryption_configuration" "warehouse" {
  bucket = aws_s3_bucket.warehouse.id
  rule {
    apply_server_side_encryption_by_default { sse_algorithm = "aws:kms" }
  }
}

resource "aws_s3_bucket_public_access_block" "warehouse" {
  bucket                  = aws_s3_bucket.warehouse.id
  block_public_acls       = true
  block_public_policy     = true
  ignore_public_acls      = true
  restrict_public_buckets = true
}

# Separate bucket for MSAF JAR and SQL artifacts
resource "aws_s3_bucket" "artifacts" {
  bucket        = "${var.team_name}-flink-artifacts"
  force_destroy = false
  tags          = { Name = "${var.team_name}-flink-artifacts", Team = var.team_name }
}

resource "aws_s3_bucket_server_side_encryption_configuration" "artifacts" {
  bucket = aws_s3_bucket.artifacts.id
  rule {
    apply_server_side_encryption_by_default { sse_algorithm = "aws:kms" }
  }
}

# resource "aws_glue_catalog_database" "main" {
#   name        = var.glue_database_name
#   description = "Iceberg table metadata for ${var.team_name}"
# }

resource "aws_lakeformation_resource" "warehouse" {
  arn = aws_s3_bucket.warehouse.arn
}

# Lake Formation DB-level grants removed — IAM policies on flink_msaf_role and dbt_runner_role
# provide the necessary access for this PoC without requiring LF admin setup.
# resource "aws_lakeformation_permissions" "msaf_table" { ... }
# resource "aws_lakeformation_permissions" "dbt_table" { ... }
