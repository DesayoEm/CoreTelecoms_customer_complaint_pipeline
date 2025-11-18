
resource "aws_iam_user" "airflow_local" {
  name = "airflow-local"

  tags = {
    Project = "Clinexa"
    Purpose = "Local development access to S3"
  }
}

resource "aws_iam_access_key" "airflow_local" {
  user = aws_iam_user.airflow_local.name
}

output "airflow_access_key_id" {
  value     = aws_iam_access_key.airflow_local.id
  sensitive = true
}

output "airflow_secret_access_key" {
  value     = aws_iam_access_key.airflow_local.secret
  sensitive = true
}

resource "aws_iam_policy" "s3_access" {
  name        = "ctgov-s3-access"
  description = "Allow read/write to ctgov bucket"

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Effect = "Allow"
        Action = [
          "s3:PutObject",
          "s3:GetObject",
          "s3:DeleteObject",
          "s3:ListBucket"
        ]
        Resource = [
          aws_s3_bucket.clinexa-ctgov-staging.arn,
          "${aws_s3_bucket.clinexa-ctgov-staging.arn}/*"
        ]
      }
    ]
  })
}

resource "aws_iam_user_policy_attachment" "airflow_s3" {
  user       = aws_iam_user.airflow_local.name
  policy_arn = aws_iam_policy.s3_access.arn
}