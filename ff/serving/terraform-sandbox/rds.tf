

resource "aws_db_instance" "postgres" {
  identifier             = local.cluster_name
  instance_class         = "db.t3.micro"
  allocated_storage      = 5
  engine                 = "postgres"
  engine_version         = "13.4"
  username               = "username"
  password               = "password"
  db_subnet_group_name   = aws_db_subnet_group.private_subnet_group.name
  publicly_accessible    = true
  skip_final_snapshot    = true
}