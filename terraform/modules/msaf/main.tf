resource "aws_cloudwatch_log_group" "msaf" {
  name              = "/aws/kinesisanalytics/${var.team_name}-flink"
  retention_in_days = 14
}

resource "aws_kinesisanalyticsv2_application" "flink" {
  name                   = "${var.team_name}-flink-kafka-iceberg"
  runtime_environment    = "FLINK-1_20"
  service_execution_role = var.flink_msaf_role_arn

  application_configuration {
    application_code_configuration {
      code_content_type = "ZIPFILE"
      code_content {
        s3_content_location {
          bucket_arn = "arn:aws:s3:::${var.team_name}-flink-artifacts"
          file_key   = var.flink_job_jar_s3_path != "" ? var.flink_job_jar_s3_path : "placeholder/flink-job-all.jar"
        }
      }
    }

    flink_application_configuration {
      checkpoint_configuration {
        configuration_type = "CUSTOM"
        checkpointing_enabled          = true
        checkpoint_interval            = 60000
        min_pause_between_checkpoints  = 5000
      }

      monitoring_configuration {
        configuration_type = "CUSTOM"
        log_level          = "INFO"
        metrics_level      = "TASK"
      }

      parallelism_configuration {
        configuration_type   = "CUSTOM"
        parallelism          = var.flink_parallelism
        parallelism_per_kpu  = var.flink_parallelism_per_kpu
        auto_scaling_enabled = true
      }
    }

    # Application properties injected as env vars into the Flink job
    environment_properties {
      property_group {
        property_group_id = "FlinkApplicationProperties"
        property_map = {
          "KAFKA_BOOTSTRAP_SERVERS" = var.kafka_bootstrap_servers
          "ICEBERG_WAREHOUSE_PATH"  = "s3://${var.s3_warehouse_bucket}/"
          "AWS_REGION"              = var.aws_region
        }
      }
    }

    vpc_configuration {
      subnet_ids         = var.private_subnet_ids
      security_group_ids = [var.flink_security_group_id]
    }
  }

  cloudwatch_logging_options {
    log_stream_arn = aws_cloudwatch_log_stream.msaf.arn
  }

  tags = { Team = var.team_name }
}

resource "aws_cloudwatch_log_stream" "msaf" {
  name           = "${var.team_name}-flink-stream"
  log_group_name = aws_cloudwatch_log_group.msaf.name
}

# Alarm: consumer falling behind
resource "aws_cloudwatch_metric_alarm" "lag" {
  alarm_name          = "${var.team_name}-flink-consumer-lag"
  comparison_operator = "GreaterThanThreshold"
  evaluation_periods  = 3
  metric_name         = "millisBehindLatest"
  namespace           = "AWS/KinesisAnalytics"
  period              = 60
  statistic           = "Maximum"
  threshold           = 300000
  dimensions = {
    Application = aws_kinesisanalyticsv2_application.flink.name
  }
}
