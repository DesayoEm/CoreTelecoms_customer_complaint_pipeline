resource "aws_secretsmanager_secret" "google_cloud_secretsv2" {
  name = "google_cloud_credv2"
}

resource "aws_secretsmanager_secret_version" "google_cloud_secrets_version" {
  secret_id     = aws_secretsmanager_secret.google_cloud_secretsv2.id
  secret_string = file("${path.module}/service_account.json")
}