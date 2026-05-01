terraform {
  required_version = ">= 1.5"
  required_providers {
    aws = {
      source  = "hashicorp/aws"
      version = "~> 5.0"
    }
  }
  # Teams should configure a remote backend (S3 + DynamoDB) — example:
  # backend "s3" {
  #   bucket         = "<team>-terraform-state"
  #   key            = "flink-pipeline/terraform.tfstate"
  #   region         = "us-east-1"
  #   dynamodb_table = "<team>-terraform-locks"
  #   encrypt        = true
  # }
}

provider "aws" {
  region = var.aws_region
}

module "networking" {
  source             = "./modules/networking"
  team_name          = var.team_name
  vpc_cidr           = var.vpc_cidr
  availability_zones = var.availability_zones
  aws_region         = var.aws_region
}

module "iam" {
  source              = "./modules/iam"
  team_name           = var.team_name
  aws_account         = var.aws_account
  aws_region          = var.aws_region
  s3_warehouse_bucket = var.s3_warehouse_bucket
}

module "storage" {
  source              = "./modules/storage"
  team_name           = var.team_name
  s3_warehouse_bucket = var.s3_warehouse_bucket
  glue_database_name  = var.glue_database_name
  flink_msaf_role_arn = module.iam.flink_msaf_role_arn
  dbt_runner_role_arn = module.iam.dbt_runner_role_arn
}

module "msk" {
  source                     = "./modules/msk"
  team_name                  = var.team_name
  vpc_id                     = module.networking.vpc_id
  private_subnet_ids         = module.networking.private_subnet_ids
  msk_security_group_id      = module.networking.msk_security_group_id
  kafka_instance_type        = var.kafka_instance_type
  kafka_broker_count         = var.kafka_broker_count
  kafka_topics               = var.kafka_topics
  kafka_partitions_per_topic = var.kafka_partitions_per_topic
}

module "msaf" {
  source                    = "./modules/msaf"
  team_name                 = var.team_name
  aws_region                = var.aws_region
  flink_msaf_role_arn       = module.iam.flink_msaf_role_arn
  s3_warehouse_bucket       = var.s3_warehouse_bucket
  flink_parallelism         = var.flink_parallelism
  flink_parallelism_per_kpu = var.flink_parallelism_per_kpu
  kafka_bootstrap_servers   = module.msk.bootstrap_brokers_tls
  flink_job_jar_s3_path     = var.flink_job_jar_s3_path
  flink_sql_s3_path         = var.flink_sql_s3_path
  vpc_id                    = module.networking.vpc_id
  private_subnet_ids        = module.networking.private_subnet_ids
  flink_security_group_id   = module.networking.flink_security_group_id
}

module "ecs_sql_gateway" {
  source                    = "./modules/ecs_sql_gateway"
  team_name                 = var.team_name
  aws_region                = var.aws_region
  aws_account               = var.aws_account
  vpc_id                    = module.networking.vpc_id
  private_subnet_ids        = module.networking.private_subnet_ids
  public_subnet_ids         = module.networking.public_subnet_ids
  ecs_task_role_arn         = module.iam.ecs_task_role_arn
  kafka_bootstrap_servers   = module.msk.bootstrap_brokers_tls
  s3_warehouse_bucket       = var.s3_warehouse_bucket
  glue_database_name        = var.glue_database_name
}

module "ecs_dbt" {
  source                  = "./modules/ecs_dbt"
  team_name               = var.team_name
  aws_region              = var.aws_region
  aws_account             = var.aws_account
  vpc_id                  = module.networking.vpc_id
  private_subnet_ids      = module.networking.private_subnet_ids
  dbt_runner_role_arn     = module.iam.dbt_runner_role_arn
  flink_gateway_host      = module.ecs_sql_gateway.alb_dns_name
  s3_warehouse_bucket     = var.s3_warehouse_bucket
  glue_database_name      = var.glue_database_name
  dbt_schedule            = var.dbt_schedule
}
