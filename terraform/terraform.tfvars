team_name   = "sai-flink"
aws_region  = "us-east-1"
aws_account = "890319878562"

vpc_cidr           = "10.1.0.0/16"
availability_zones = ["us-east-1a", "us-east-1b", "us-east-1c"]

kafka_instance_type        = "kafka.m5.large"
kafka_broker_count         = 3
kafka_topics               = ["orders_topic"]
kafka_partitions_per_topic = 6

flink_parallelism         = 6
flink_parallelism_per_kpu = 1

s3_warehouse_bucket = "sai-flink-iceberg-warehouse-890319878562"
glue_database_name  = "default"

dbt_schedule = "rate(15 minutes)"
