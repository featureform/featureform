resource "aws_db_subnet_group" "public_subnet_group" {
  name       =  "${local.cluster_name}-sb-subnet"
  subnet_ids = module.vpc.public_subnets

  tags = {
    Name = local.cluster_name
  }
}

resource "aws_db_instance" "postgres" {
  identifier             = local.cluster_name
  instance_class         = "db.t3.medium"
  allocated_storage      = 5
  engine                 = "postgres"
  engine_version         = "13.4"
  username               = "username"
  password               = "password"
  db_subnet_group_name   = aws_db_subnet_group.public_subnet_group.name
  publicly_accessible    = true
  skip_final_snapshot    = true
  security_group_names = [aws_security_group.all_worker_mgmt.name]
}