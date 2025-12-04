resource "aws_db_instance" "ctp_db_instance" {
  allocated_storage           = 100
  db_name                     = "ct_postgres"
  engine                      = "postgres"
  port                        = 5432
  engine_version              = "16.11"
  instance_class              = "db.m5.large"
  multi_az                    = false
  db_subnet_group_name        = aws_db_subnet_group.ctp_db_subnet_group.name
  vpc_security_group_ids      = [aws_security_group.database_security_group.id]
  username                    = "ctpadmin"
  manage_master_user_password = true
  skip_final_snapshot         = false
  publicly_accessible         = true

  tags = {
    Name        = "CoreTelecoms DB"
    Environment = "Development"
  }
}

 # skip_final_snapshot         = false  #
 #  final_snapshot_identifier   = "ctp-db-snapshot-${formatdate("YYYY-MM-DD-hhmm", timestamp())}"