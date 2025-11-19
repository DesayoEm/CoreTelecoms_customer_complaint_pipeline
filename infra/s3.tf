resource "aws_s3_bucket" "customers-staging" {
  bucket = "coretelecoms-customers-staging"
  force_destroy = true

  tags = {
    Name        = "CT gov bucket"
    Environment = "Test"
  }
}

resource "aws_s3_bucket_versioning" "ctgov_versioning" {
  bucket = aws_s3_bucket.customers-staging.id
  versioning_configuration {
    status = "Enabled"
  }

  # lifecycle {
  #   prevent_destroy = true
  # }
}


resource "aws_s3_bucket_lifecycle_configuration" "ct_gov_archive_lifecycle" {
  bucket = aws_s3_bucket.customers-staging.id

  rule {
    id     = "TransitionToStandardIA"
    status = "Enabled"

    transition {
      days          = 30
      storage_class = "STANDARD_IA"
    }
  }

  }

