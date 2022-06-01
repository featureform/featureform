resource "aws_elasticache_subnet_group" "ec_subnet_grp" {
  name       = "${local.cluster_name}-cache-subnet"
  subnet_ids = module.vpc.public_subnets
}

resource "aws_elasticache_cluster" "cluster" {
  cluster_id           = local.cluster_name
  engine               = "redis"
  node_type            = "cache.m4.large"
  num_cache_nodes      = 1
  parameter_group_name = "default.redis3.2"
  engine_version       = "3.2.10"
  port                 = 6379
  subnet_group_name = aws_elasticache_subnet_group.ec_subnet_grp.name
  security_group_names = [aws_security_group.all_worker_mgmt.name]
}
