output "application_arn" { value = aws_kinesisanalyticsv2_application.flink.arn }
output "application_name"{ value = aws_kinesisanalyticsv2_application.flink.name }
output "artifacts_bucket"{ value = "${var.team_name}-flink-artifacts" }
