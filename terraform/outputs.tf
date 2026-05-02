output "flink_gateway_url" {
  description = "URL for the Flink SQL Gateway (use as FLINK_GATEWAY_HOST in dbt profiles)."
  value       = "https://${module.ecs_sql_gateway.alb_dns_name}"
}

output "msk_bootstrap_brokers" {
  description = "MSK broker endpoints (TLS). Use as KAFKA_BOOTSTRAP_SERVERS."
  value       = module.msk.bootstrap_brokers_tls
  sensitive   = true
}

output "s3_warehouse_path" {
  description = "S3 URI for the Iceberg data warehouse."
  value       = "s3://${var.s3_warehouse_bucket}/"
}

# output "glue_database_arn" {
#   description = "ARN of the Glue database holding Iceberg table metadata."
#   value       = module.storage.glue_database_arn
# }

output "ecr_sql_gateway_repository_url" {
  description = "ECR repository URL for the Flink SQL Gateway image."
  value       = module.ecs_sql_gateway.ecr_repository_url
}

output "ecr_dbt_repository_url" {
  description = "ECR repository URL for the dbt runner image."
  value       = module.ecs_dbt.ecr_repository_url
}

output "flink_artifacts_bucket" {
  description = "S3 bucket for MSAF JAR and SQL artifacts."
  value       = module.msaf.artifacts_bucket
}
