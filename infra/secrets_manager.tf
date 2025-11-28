resource "aws_secretsmanager_secret" "google_cloud_secrets" {
  name = "google_cloud_credv5"
}

resource "aws_secretsmanager_secret_version" "google_cloud_secrets_version" {
  secret_id     = aws_secretsmanager_secret.google_cloud_secrets.id
  secret_string = file("${path.module}/service_account.json")
}

output "db_secret_arn" {
  value       = aws_db_instance.ctp_db_instance.master_user_secret[0].secret_arn
  description = "ARN of the RDS master password secret"
}

output "db_endpoint" {
  value       = aws_db_instance.ctp_db_instance.endpoint
  description = "RDS instance endpoint"
}