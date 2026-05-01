output "ecr_repository_url"     { value = aws_ecr_repository.dbt.repository_url }
output "task_definition_arn"    { value = aws_ecs_task_definition.dbt.arn }
