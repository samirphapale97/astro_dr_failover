# -----------------------------------------------------------------------------
# Secrets Module — KMS Key + Secrets Manager Secret
# Creates a KMS key for encryption and a Secrets Manager secret with provided values.
# -----------------------------------------------------------------------------

resource "aws_kms_key" "this" {
  description             = "KMS key for ${var.project}-${var.environment} secrets in ${var.region}"
  enable_key_rotation     = true
  deletion_window_in_days = var.kms_deletion_window

  tags = merge(var.tags, {
    Name        = "${var.project}-${var.environment}-${var.region}-secrets-key"
    Project     = var.project
    Environment = var.environment
    Region      = var.region
    ManagedBy   = "Terraform"
  })
}

resource "aws_kms_alias" "this" {
  name          = "alias/${var.project}-${var.environment}-${var.region}-secrets"
  target_key_id = aws_kms_key.this.key_id
}

resource "aws_secretsmanager_secret" "this" {
  name                    = "${var.project}-${var.environment}-${var.secret_name}"
  kms_key_id              = aws_kms_key.this.arn
  recovery_window_in_days = var.secret_recovery_window
  description             = "Airflow configuration secret for ${var.project}-${var.environment} in ${var.region}"

  tags = merge(var.tags, {
    Name        = "${var.project}-${var.environment}-${var.secret_name}"
    Project     = var.project
    Environment = var.environment
    Region      = var.region
    ManagedBy   = "Terraform"
  })
}

resource "aws_secretsmanager_secret_version" "this" {
  secret_id     = aws_secretsmanager_secret.this.id
  secret_string = jsonencode(var.secret_values)
}
