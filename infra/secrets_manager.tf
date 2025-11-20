resource "aws_secretsmanager_secret" "google_cloud_secrets" {
  name = "google_cloud_cred"
}

resource "aws_secretsmanager_secret_version" "google_cloud_secrets_version" {
  secret_id     = aws_secretsmanager_secret.google_cloud_secrets.id
  secret_string = file("${path.module}/service_account.json")
}