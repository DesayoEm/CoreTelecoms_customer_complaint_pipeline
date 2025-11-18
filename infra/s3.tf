resource "aws_s3_bucket" "clinexa-ctgov-staging" {
  bucket = "clinexa-ctgov-staging"
  force_destroy = true #will be disabled in prod

  tags = {
    Name        = "CT gov bucket"
    Environment = "Test"
  }
}

resource "aws_s3_bucket_versioning" "ctgov_versioning" {
  bucket = aws_s3_bucket.clinexa-ctgov-staging.id
  versioning_configuration {
    status = "Enabled"
  }

  # lifecycle {
  #   prevent_destroy = true
  # }
}


resource "aws_s3_bucket_lifecycle_configuration" "ct_gov_archive_lifecycle" {
  bucket = aws_s3_bucket.clinexa-ctgov-staging.id

  rule {
    id     = "TransitionToDeepArchive"
    status = "Enabled"

    transition {
      days          = 7
      storage_class = "DEEP_ARCHIVE"
    }
  }

  }

