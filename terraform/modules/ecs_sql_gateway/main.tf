resource "aws_ecr_repository" "sql_gateway" {
  name                 = "${var.team_name}/flink-sql-gateway"
  image_tag_mutability = "MUTABLE"
  image_scanning_configuration { scan_on_push = true }
  tags = { Team = var.team_name }
}

resource "aws_ecs_cluster" "main" {
  name = "${var.team_name}-cluster"
  setting {
    name  = "containerInsights"
    value = "enabled"
  }
}

resource "aws_cloudwatch_log_group" "sql_gateway" {
  name              = "/ecs/${var.team_name}/sql-gateway"
  retention_in_days = 7
}

resource "aws_security_group" "ecs" {
  name   = "${var.team_name}-sqlgw-ecs-sg"
  vpc_id = var.vpc_id
  ingress {
    from_port   = 8083
    to_port     = 8083
    protocol    = "tcp"
    cidr_blocks = ["10.0.0.0/8"]
  }
  egress {
    from_port   = 0
    to_port     = 0
    protocol    = "-1"
    cidr_blocks = ["0.0.0.0/0"]
  }
  tags = { Name = "${var.team_name}-sqlgw-ecs-sg" }
}

resource "aws_ecs_task_definition" "sql_gateway" {
  family                   = "${var.team_name}-sql-gateway"
  requires_compatibilities = ["FARGATE"]
  network_mode             = "awsvpc"
  cpu                      = "2048"
  memory                   = "4096"
  task_role_arn            = var.ecs_task_role_arn
  execution_role_arn       = "arn:aws:iam::${var.aws_account}:role/${var.team_name}-ecs-execution-role"

  container_definitions = jsonencode([{
    name  = "sql-gateway"
    image = "${aws_ecr_repository.sql_gateway.repository_url}:latest"
    command = ["start_flink_sql_gateway"]
    portMappings = [{ containerPort = 8083, protocol = "tcp" }]
    environment = [
      { name = "AWS_REGION",            value = var.aws_region },
      { name = "ICEBERG_WAREHOUSE_PATH",value = "s3://${var.s3_warehouse_bucket}/" },
      { name = "GLUE_DATABASE",         value = var.glue_database_name },
      { name = "FLINK_PROPERTIES",      value = "rest.address: 0.0.0.0\nrest.bind-address: 0.0.0.0\ntaskmanager.numberOfTaskSlots: 4" }
    ]
    logConfiguration = {
      logDriver = "awslogs"
      options = {
        "awslogs-group"         = aws_cloudwatch_log_group.sql_gateway.name
        "awslogs-region"        = var.aws_region
        "awslogs-stream-prefix" = "sql-gateway"
      }
    }
    healthCheck = {
      command     = ["CMD-SHELL", "curl -f http://localhost:8083/v1/info || exit 1"]
      interval    = 30
      timeout     = 5
      retries     = 3
      startPeriod = 60
    }
  }])
}

# ALB for SQL Gateway — HTTP (port 8083) for internal VPC access.
# For HTTPS, add an ACM certificate and change the listener to port 443/HTTPS.
resource "aws_lb" "sql_gateway" {
  name               = "${var.team_name}-sqlgw-alb"
  internal           = true
  load_balancer_type = "application"
  subnets            = var.private_subnet_ids
  security_groups    = [aws_security_group.alb.id]
  tags               = { Team = var.team_name }
}

resource "aws_security_group" "alb" {
  name   = "${var.team_name}-sqlgw-alb-sg"
  vpc_id = var.vpc_id
  ingress {
    from_port   = 8083
    to_port     = 8083
    protocol    = "tcp"
    cidr_blocks = ["10.0.0.0/8"]
  }
  egress {
    from_port   = 0
    to_port     = 0
    protocol    = "-1"
    cidr_blocks = ["0.0.0.0/0"]
  }
  tags = { Name = "${var.team_name}-sqlgw-alb-sg" }
}

resource "aws_lb_target_group" "sql_gateway" {
  name        = "${var.team_name}-sqlgw-tg"
  port        = 8083
  protocol    = "HTTP"
  vpc_id      = var.vpc_id
  target_type = "ip"

  health_check {
    path                = "/v1/info"
    interval            = 30
    timeout             = 5
    healthy_threshold   = 2
    unhealthy_threshold = 3
  }
}

resource "aws_lb_listener" "http" {
  load_balancer_arn = aws_lb.sql_gateway.arn
  port              = 8083
  protocol          = "HTTP"

  default_action {
    type             = "forward"
    target_group_arn = aws_lb_target_group.sql_gateway.arn
  }
}

resource "aws_ecs_service" "sql_gateway" {
  name            = "${var.team_name}-sql-gateway"
  cluster         = aws_ecs_cluster.main.id
  task_definition = aws_ecs_task_definition.sql_gateway.arn
  desired_count   = 1
  launch_type     = "FARGATE"

  network_configuration {
    subnets          = var.private_subnet_ids
    security_groups  = [aws_security_group.ecs.id]
    assign_public_ip = false
  }

  load_balancer {
    target_group_arn = aws_lb_target_group.sql_gateway.arn
    container_name   = "sql-gateway"
    container_port   = 8083
  }

  depends_on = [aws_lb_listener.http]
}

# Auto-scaling for SQL Gateway ECS service
resource "aws_appautoscaling_target" "sql_gateway" {
  max_capacity       = 5
  min_capacity       = 1
  resource_id        = "service/${aws_ecs_cluster.main.name}/${aws_ecs_service.sql_gateway.name}"
  scalable_dimension = "ecs:service:DesiredCount"
  service_namespace  = "ecs"
  depends_on         = [aws_ecs_service.sql_gateway]
}

resource "aws_appautoscaling_policy" "sql_gateway_cpu" {
  name               = "${var.team_name}-sqlgw-cpu-scaling"
  policy_type        = "TargetTrackingScaling"
  resource_id        = aws_appautoscaling_target.sql_gateway.resource_id
  scalable_dimension = aws_appautoscaling_target.sql_gateway.scalable_dimension
  service_namespace  = aws_appautoscaling_target.sql_gateway.service_namespace

  target_tracking_scaling_policy_configuration {
    predefined_metric_specification {
      predefined_metric_type = "ECSServiceAverageCPUUtilization"
    }
    target_value       = 60.0
    scale_in_cooldown  = 300
    scale_out_cooldown = 60
  }
}
