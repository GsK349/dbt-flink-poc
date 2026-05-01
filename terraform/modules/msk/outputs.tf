output "cluster_arn"              { value = aws_msk_cluster.main.arn }
output "bootstrap_brokers_tls"    { value = aws_msk_cluster.main.bootstrap_brokers_tls }
output "bootstrap_brokers_iam"    { value = aws_msk_cluster.main.bootstrap_brokers_sasl_iam }
output "kafka_bootstrap_secret_arn"{ value = aws_secretsmanager_secret.kafka_bootstrap.arn }
