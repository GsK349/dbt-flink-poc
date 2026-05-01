output "warehouse_bucket_arn"  { value = aws_s3_bucket.warehouse.arn }
output "artifacts_bucket"      { value = aws_s3_bucket.artifacts.bucket }
output "glue_database_arn"     { value = aws_glue_catalog_database.main.arn }
