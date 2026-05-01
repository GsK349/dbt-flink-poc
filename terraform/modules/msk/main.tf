resource "aws_msk_cluster" "main" {
  cluster_name           = "${var.team_name}-kafka"
  kafka_version          = "3.6.0"
  number_of_broker_nodes = var.kafka_broker_count

  broker_node_group_info {
    instance_type   = var.kafka_instance_type
    client_subnets  = var.private_subnet_ids
    security_groups = [var.msk_security_group_id]

    storage_info {
      ebs_storage_info {
        volume_size = 1000
        # Auto-scaling: triggers at 70% usage, grows by 10 GiB, max 16384 GiB
        provisioned_throughput { enabled = false }
      }
    }
  }

  client_authentication {
    sasl { iam = true }
    tls {}
  }

  encryption_info {
    encryption_in_transit {
      client_broker = "TLS"
      in_cluster    = true
    }
  }

  enhanced_monitoring = "PER_BROKER"

  open_monitoring {
    prometheus {
      jmx_exporter  { enabled_in_broker = true }
      node_exporter { enabled_in_broker = true }
    }
  }

  logging {
    broker_logs {
      cloudwatch_logs {
        enabled   = true
        log_group = aws_cloudwatch_log_group.msk.name
      }
    }
  }

  tags = { Team = var.team_name }
}

resource "aws_cloudwatch_log_group" "msk" {
  name              = "/aws/msk/${var.team_name}"
  retention_in_days = 7
}

# Store bootstrap servers in Secrets Manager for MSAF and ECS to consume
resource "aws_secretsmanager_secret" "kafka_bootstrap" {
  name = "${var.team_name}/kafka-bootstrap-servers"
}

resource "aws_secretsmanager_secret_version" "kafka_bootstrap" {
  secret_id     = aws_secretsmanager_secret.kafka_bootstrap.id
  secret_string = aws_msk_cluster.main.bootstrap_brokers_sasl_iam
}

# CloudWatch alarm for consumer lag
resource "aws_cloudwatch_metric_alarm" "consumer_lag" {
  alarm_name          = "${var.team_name}-kafka-consumer-lag"
  comparison_operator = "GreaterThanThreshold"
  evaluation_periods  = 3
  metric_name         = "EstimatedMaxTimeLag"
  namespace           = "AWS/Kafka"
  period              = 60
  statistic           = "Maximum"
  threshold           = 300000
  alarm_description   = "MSK consumer lag exceeds 5 minutes"
  dimensions = {
    ClusterName = aws_msk_cluster.main.cluster_name
  }
}
