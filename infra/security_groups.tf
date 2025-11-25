# WEBSERVER SECURITY GROUP
resource "aws_security_group" "webserver_security_group" {
  name        = "webserver security group"
  description = "Enable HTTP/HTTPS access"
  vpc_id      = aws_vpc.coretelecoms.id

  tags = {
    Name = "Web server SG"
  }
}

resource "aws_vpc_security_group_ingress_rule" "webserver_http" {
  security_group_id = aws_security_group.webserver_security_group.id
  cidr_ipv4         = "0.0.0.0/0"
  from_port         = 80
  ip_protocol       = "tcp"
  to_port           = 80
}

resource "aws_vpc_security_group_ingress_rule" "webserver_https" {
  security_group_id = aws_security_group.webserver_security_group.id
  cidr_ipv4         = "0.0.0.0/0"
  from_port         = 443
  ip_protocol       = "tcp"
  to_port           = 443
}

resource "aws_vpc_security_group_egress_rule" "webserver_all" {
  security_group_id = aws_security_group.webserver_security_group.id
  cidr_ipv4         = "0.0.0.0/0"
  ip_protocol       = "-1"
}

# DATABASE SECURITY GROUP
resource "aws_security_group" "database_security_group" {
  name        = "database security group"
  description = "Enable Postgres access on port 5432"
  vpc_id      = aws_vpc.coretelecoms.id

  tags = {
    Name = "Database SG"
  }
}

resource "aws_vpc_security_group_ingress_rule" "database_postgres" {
  security_group_id = aws_security_group.database_security_group.id
  cidr_ipv4         = "0.0.0.0/0"
  # TODO: Restrict after deploying Airflow in VPC
  #aws_vpc.coretelecoms.cidr_block

  from_port   = 5432
  ip_protocol = "tcp"
  to_port     = 5432
}

resource "aws_vpc_security_group_egress_rule" "database_all" {
  security_group_id = aws_security_group.database_security_group.id
  cidr_ipv4         = "0.0.0.0/0"
  ip_protocol       = "-1"
}