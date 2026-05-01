variable "team_name"                  { type = string }
variable "vpc_id"                    { type = string }
variable "private_subnet_ids"        { type = list(string) }
variable "msk_security_group_id"     { type = string }
variable "kafka_instance_type"       { type = string }
variable "kafka_broker_count"        { type = number }
variable "kafka_topics"              { type = list(string) }
variable "kafka_partitions_per_topic"{ type = number }
