resource "aws_ecr_repository" "dbt" {
  name                 = "${var.team_name}/flink-dbt"
  image_tag_mutability = "MUTABLE"
  image_scanning_configuration { scan_on_push = true }
  tags = { Team = var.team_name }
}

resource "aws_cloudwatch_log_group" "dbt" {
  name              = "/ecs/${var.team_name}/dbt"
  retention_in_days = 7
}

resource "aws_ecs_task_definition" "dbt" {
  family                   = "${var.team_name}-dbt"
  requires_compatibilities = ["FARGATE"]
  network_mode             = "awsvpc"
  cpu                      = "1024"
  memory                   = "2048"
  task_role_arn            = var.dbt_runner_role_arn
  execution_role_arn       = "arn:aws:iam::${var.aws_account}:role/${var.team_name}-ecs-execution-role"

  container_definitions = jsonencode([{
    name    = "dbt-runner"
    image   = "${aws_ecr_repository.dbt.repository_url}:latest"
    command = ["dbt", "run", "--profiles-dir", "/dbt", "--project-dir", "/project", "--target", "aws"]
    environment = [
      { name = "FLINK_GATEWAY_HOST",    value = var.flink_gateway_host },
      { name = "DBT_TARGET",            value = "aws" },
      { name = "ICEBERG_WAREHOUSE_PATH",value = "s3://${var.s3_warehouse_bucket}/" },
      { name = "AWS_REGION",            value = var.aws_region }
    ]
    logConfiguration = {
      logDriver = "awslogs"
      options = {
        "awslogs-group"         = aws_cloudwatch_log_group.dbt.name
        "awslogs-region"        = var.aws_region
        "awslogs-stream-prefix" = "dbt"
      }
    }
  }])
}

resource "aws_scheduler_schedule" "dbt" {
  name = "${var.team_name}-dbt-schedule"

  flexible_time_window { mode = "OFF" }

  schedule_expression = var.dbt_schedule

  target {
    arn      = "arn:aws:ecs:${var.aws_region}:${var.aws_account}:cluster/${var.team_name}-cluster"
    role_arn = aws_iam_role.scheduler.arn

    ecs_parameters {
      task_definition_arn = aws_ecs_task_definition.dbt.arn
      launch_type         = "FARGATE"
      network_configuration {
        assign_public_ip = false
        subnets          = var.private_subnet_ids
      }
    }
  }
}

data "aws_iam_policy_document" "scheduler_assume" {
  statement {
    actions = ["sts:AssumeRole"]
    principals {
      type        = "Service"
      identifiers = ["scheduler.amazonaws.com"]
    }
  }
}

resource "aws_iam_role" "scheduler" {
  name               = "${var.team_name}-dbt-scheduler-role"
  assume_role_policy = data.aws_iam_policy_document.scheduler_assume.json
}

resource "aws_iam_role_policy" "scheduler" {
  name = "${var.team_name}-dbt-scheduler-policy"
  role = aws_iam_role.scheduler.id
  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Effect   = "Allow"
      Action   = ["ecs:RunTask", "iam:PassRole"]
      Resource = "*"
    }]
  })
}
