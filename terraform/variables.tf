variable "team_name" {
  description = "Short identifier for the team. Used to namespace all AWS resources."
  type        = string
}

variable "aws_region" {
  description = "AWS region to deploy into."
  type        = string
  default     = "us-east-1"
}

variable "aws_account" {
  description = "AWS account ID."
  type        = string
}

variable "vpc_cidr" {
  description = "CIDR block for the VPC."
  type        = string
  default     = "10.1.0.0/16"
}

variable "availability_zones" {
  description = "List of AZs to deploy MSK brokers into (minimum 2, recommend 3)."
  type        = list(string)
  default     = ["us-east-1a", "us-east-1b", "us-east-1c"]
}

variable "kafka_instance_type" {
  description = "MSK broker instance type."
  type        = string
  default     = "kafka.m5.large"
}

variable "kafka_broker_count" {
  description = "Number of MSK brokers. Must equal the number of AZs."
  type        = number
  default     = 3
}

variable "kafka_topics" {
  description = "List of Kafka topic names to create."
  type        = list(string)
  default     = ["orders_topic"]
}

variable "kafka_partitions_per_topic" {
  description = "Number of partitions per topic. Sets the MSAF parallelism ceiling."
  type        = number
  default     = 6
}

variable "flink_parallelism" {
  description = "MSAF application parallelism. Should equal kafka_partitions_per_topic."
  type        = number
  default     = 6
}

variable "flink_parallelism_per_kpu" {
  description = "Flink operators per KPU. Set to 1 so parallelism == KPU count."
  type        = number
  default     = 1
}

variable "s3_warehouse_bucket" {
  description = "S3 bucket name for the Iceberg data warehouse."
  type        = string
}

variable "glue_database_name" {
  description = "Glue database name for Iceberg table metadata."
  type        = string
  default     = "default"
}

variable "dbt_schedule" {
  description = "EventBridge Scheduler expression for dbt runs."
  type        = string
  default     = "rate(15 minutes)"
}

variable "flink_job_jar_s3_path" {
  description = "S3 URI of the fat JAR for the MSAF application (set by CI/CD after mvn package)."
  type        = string
  default     = ""
}

variable "flink_sql_s3_path" {
  description = "S3 URI of the SQL script for the MSAF SQL application."
  type        = string
  default     = ""
}
