resource "aws_db_subnet_group" "ctp_db_subnet_group" {
  name       = "rds_subnet_group"
  subnet_ids = [aws_subnet.subnet_az1.id, aws_subnet.subnet_az2.id]

  tags = {
    Name = "CoreTelecoms DB subnet group"
  }
}