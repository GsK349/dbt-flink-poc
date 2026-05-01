output "flink_msaf_role_arn"   { value = aws_iam_role.flink_msaf.arn }
output "ecs_task_role_arn"     { value = aws_iam_role.ecs_task.arn }
output "dbt_runner_role_arn"   { value = aws_iam_role.dbt_runner.arn }
output "ecs_execution_role_arn"{ value = aws_iam_role.ecs_execution.arn }
