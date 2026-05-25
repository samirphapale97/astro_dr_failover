# -----------------------------------------------------------------------------
# Root Outputs
# -----------------------------------------------------------------------------

output "primary_secret_arn" {
  description = "ARN of the Secrets Manager secret in the primary region."
  value       = module.primary_secrets.secret_arn
}

output "dr_secret_arn" {
  description = "ARN of the Secrets Manager secret in the DR region."
  value       = module.dr_secrets.secret_arn
}

output "primary_kms_key_arn" {
  description = "ARN of the KMS key used for secrets encryption in the primary region."
  value       = module.primary_secrets.kms_key_arn
}

output "dr_kms_key_arn" {
  description = "ARN of the KMS key used for secrets encryption in the DR region."
  value       = module.dr_secrets.kms_key_arn
}

output "health_check_id" {
  description = "ID of the Route53 health check monitoring the primary region."
  value       = module.health_check.health_check_id
}

output "failover_lambda_arn" {
  description = "ARN of the auto-failover Lambda function."
  value       = module.failover_lambda.lambda_arn
}

output "sns_topic_arn" {
  description = "ARN of the SNS topic for failover alerts."
  value       = module.health_check.sns_topic_arn
}

output "ssm_parameter_name" {
  description = "Name of the SSM parameter tracking the active region."
  value       = module.ssm.parameter_name
}

output "active_region_ssm_arn" {
  description = "ARN of the SSM parameter tracking the active region."
  value       = module.ssm.parameter_arn
}

# ─── ACID Failover Outputs ──────────────────────────────────────────────

output "sqs_queue_url" {
  description = "URL of the failover events SQS FIFO queue."
  value       = module.sqs.queue_url
}

output "sqs_dlq_url" {
  description = "URL of the failover events dead-letter queue."
  value       = module.sqs.dlq_url
}

output "dynamodb_state_table" {
  description = "Name of the DynamoDB failover state table."
  value       = module.dynamodb.state_table_name
}

output "dynamodb_lock_table" {
  description = "Name of the DynamoDB distributed lock table."
  value       = module.dynamodb.lock_table_name
}
