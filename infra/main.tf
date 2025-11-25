provider "aws" {
  region = "eu-west-2"
}

terraform {
  backend "s3" {
    bucket = "ctelecoms-terraform-bucket"
    key    = "terraform.tfstate"
    region = "eu-west-2"
  }
}


