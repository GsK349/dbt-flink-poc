data "aws_iam_policy_document" "msaf_assume" {
  statement {
    actions = ["sts:AssumeRole"]
    principals {
      type        = "Service"
      identifiers = ["kinesisanalytics.amazonaws.com"]
    }
  }
}

resource "aws_iam_role" "flink_msaf" {
  name               = "${var.team_name}-flink-msaf-role"
  assume_role_policy = data.aws_iam_policy_document.msaf_assume.json
}

resource "aws_iam_role_policy" "msaf_policy" {
  name = "${var.team_name}-msaf-policy"
  role = aws_iam_role.flink_msaf.id
  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Effect   = "Allow"
        Action   = ["s3:GetObject", "s3:PutObject", "s3:DeleteObject", "s3:ListBucket", "s3:GetBucketLocation"]
        Resource = [
          "arn:aws:s3:::${var.s3_warehouse_bucket}",
          "arn:aws:s3:::${var.s3_warehouse_bucket}/*",
          "arn:aws:s3:::${var.team_name}-flink-artifacts",
          "arn:aws:s3:::${var.team_name}-flink-artifacts/*"
        ]
      },
      {
        Effect   = "Allow"
        Action   = ["glue:*"]
        Resource = ["arn:aws:glue:${var.aws_region}:${var.aws_account}:*"]
      },
      {
        Effect   = "Allow"
        Action   = ["kafka:DescribeCluster", "kafka:GetBootstrapBrokers", "kafka:ListClusters"]
        Resource = "*"
      },
      {
        Effect   = "Allow"
        Action   = ["logs:CreateLogGroup", "logs:CreateLogDelivery", "logs:PutLogEvents", "logs:DescribeLogGroups", "logs:DescribeLogStreams"]
        Resource = "*"
      },
      {
        Effect   = "Allow"
        Action   = ["kinesisanalytics:DescribeApplication"]
        Resource = "arn:aws:kinesisanalytics:${var.aws_region}:${var.aws_account}:application/${var.team_name}-*"
      },
      {
        Effect   = "Allow"
        Action   = ["secretsmanager:GetSecretValue"]
        Resource = "arn:aws:secretsmanager:${var.aws_region}:${var.aws_account}:secret:${var.team_name}/*"
      }
    ]
  })
}

data "aws_iam_policy_document" "ecs_assume" {
  statement {
    actions = ["sts:AssumeRole"]
    principals {
      type        = "Service"
      identifiers = ["ecs-tasks.amazonaws.com"]
    }
  }
}

resource "aws_iam_role" "ecs_task" {
  name               = "${var.team_name}-ecs-task-role"
  assume_role_policy = data.aws_iam_policy_document.ecs_assume.json
}

resource "aws_iam_role_policy" "ecs_task_policy" {
  name = "${var.team_name}-ecs-task-policy"
  role = aws_iam_role.ecs_task.id
  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Effect   = "Allow"
        Action   = ["s3:GetObject", "s3:ListBucket"]
        Resource = ["arn:aws:s3:::${var.s3_warehouse_bucket}", "arn:aws:s3:::${var.s3_warehouse_bucket}/*"]
      },
      {
        Effect   = "Allow"
        Action   = ["glue:GetTable", "glue:GetTables", "glue:GetDatabase", "glue:GetDatabases", "glue:GetPartition", "glue:GetPartitions"]
        Resource = ["arn:aws:glue:${var.aws_region}:${var.aws_account}:*"]
      },
      {
        Effect   = "Allow"
        Action   = ["secretsmanager:GetSecretValue"]
        Resource = "arn:aws:secretsmanager:${var.aws_region}:${var.aws_account}:secret:${var.team_name}/*"
      }
    ]
  })
}

resource "aws_iam_role" "dbt_runner" {
  name               = "${var.team_name}-dbt-runner-role"
  assume_role_policy = data.aws_iam_policy_document.ecs_assume.json
}

resource "aws_iam_role_policy" "dbt_runner_policy" {
  name = "${var.team_name}-dbt-runner-policy"
  role = aws_iam_role.dbt_runner.id
  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Effect   = "Allow"
        Action   = ["s3:GetObject", "s3:ListBucket"]
        Resource = ["arn:aws:s3:::${var.s3_warehouse_bucket}", "arn:aws:s3:::${var.s3_warehouse_bucket}/*"]
      },
      {
        Effect   = "Allow"
        Action   = ["glue:GetTable", "glue:GetTables", "glue:GetDatabase", "glue:GetDatabases", "glue:CreateTable", "glue:UpdateTable"]
        Resource = ["arn:aws:glue:${var.aws_region}:${var.aws_account}:*"]
      }
    ]
  })
}

# ECS task execution role — allows ECS to pull images from ECR and write logs
resource "aws_iam_role" "ecs_execution" {
  name               = "${var.team_name}-ecs-execution-role"
  assume_role_policy = data.aws_iam_policy_document.ecs_assume.json
}

resource "aws_iam_role_policy_attachment" "ecs_execution_policy" {
  role       = aws_iam_role.ecs_execution.name
  policy_arn = "arn:aws:iam::aws:policy/service-role/AmazonECSTaskExecutionRolePolicy"
}
