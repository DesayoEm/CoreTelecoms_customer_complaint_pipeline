resource "aws_s3_bucket" "coretelecoms-lake" {
  bucket = "coretelecoms-data-lake"
  force_destroy = true

  tags = {
    Name        = "CoreTelecoms Bucket"
    Environment = "Test"
  }
}

resource "aws_s3_bucket_versioning" "coretelecoms_versioning" {
  bucket = aws_s3_bucket.coretelecoms-lake.id
  versioning_configuration {
    status = "Enabled"
  }

  # lifecycle {
  #   prevent_destroy = true
  # }
}


resource "aws_s3_bucket_lifecycle_configuration" "coretelecoms_lake_archive_lifecycle" {
  bucket = aws_s3_bucket.coretelecoms-lake.id

  rule {
    id     = "TransitionToStandardIA"
    status = "Enabled"

    transition {
      days          = 30
      storage_class = "STANDARD_IA"
    }
  }

  }


