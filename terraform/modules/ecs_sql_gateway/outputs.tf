output "alb_dns_name"          { value = aws_lb.sql_gateway.dns_name }
output "ecr_repository_url"    { value = aws_ecr_repository.sql_gateway.repository_url }
output "ecs_cluster_name"      { value = aws_ecs_cluster.main.name }
