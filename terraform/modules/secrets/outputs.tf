output "secret_arn" {
  description = "ARN of the Secrets Manager secret."
  value       = aws_secretsmanager_secret.this.arn
}

output "secret_name" {
  description = "Name of the Secrets Manager secret."
  value       = aws_secretsmanager_secret.this.name
}

output "kms_key_arn" {
  description = "ARN of the KMS key used for secret encryption."
  value       = aws_kms_key.this.arn
}

output "kms_key_id" {
  description = "ID of the KMS key used for secret encryption."
  value       = aws_kms_key.this.key_id
}
