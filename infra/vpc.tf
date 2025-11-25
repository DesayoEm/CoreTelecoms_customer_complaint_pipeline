resource "aws_vpc" "coretelecoms" {
  cidr_block       = "10.0.0.0/16"
  instance_tenancy = "default"

  enable_dns_support   = true
  enable_dns_hostnames = true

  tags = {
    Name = "CT data pipeline"
  }
}

resource "aws_internet_gateway" "gw" {
  vpc_id = aws_vpc.coretelecoms.id


  tags = {
    Name = "CT data pipeline"
  }
}


# Route table for public subnets
resource "aws_route_table" "public" {
  vpc_id = aws_vpc.coretelecoms.id

  route {
    cidr_block = "0.0.0.0/0"
    gateway_id = aws_internet_gateway.gw.id
  }

  tags = {
    Name = "CT Public Route Table"
  }
}

resource "aws_route_table_association" "db_subnet_1" {
  subnet_id      = aws_subnet.subnet_az1.id
  route_table_id = aws_route_table.public.id
}

resource "aws_route_table_association" "db_subnet_2" {
  subnet_id      = aws_subnet.subnet_az2.id
  route_table_id = aws_route_table.public.id
}