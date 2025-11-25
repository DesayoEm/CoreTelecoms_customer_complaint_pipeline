data "aws_availability_zones" "available_zones" {}


resource "aws_subnet" "subnet_az1" {
  vpc_id            = aws_vpc.coretelecoms.id
  cidr_block        = "10.0.0.0/24"
  availability_zone = data.aws_availability_zones.available_zones.names[0]

  tags = {
    Name = "CT data pipeline"
  }
}

resource "aws_subnet" "subnet_az2" {
  vpc_id            = aws_vpc.coretelecoms.id
  cidr_block        = "10.0.1.0/24"
  availability_zone = data.aws_availability_zones.available_zones.names[1]

  tags = {
    Name = "CT data pipeline"
  }
}



