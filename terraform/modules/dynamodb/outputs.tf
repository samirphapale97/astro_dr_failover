output "state_table_name" {
  description = "Name of the failover state DynamoDB table."
  value       = aws_dynamodb_table.failover_state.name
}

output "state_table_arn" {
  description = "ARN of the failover state DynamoDB table."
  value       = aws_dynamodb_table.failover_state.arn
}

output "lock_table_name" {
  description = "Name of the distributed lock DynamoDB table."
  value       = aws_dynamodb_table.failover_locks.name
}

output "lock_table_arn" {
  description = "ARN of the distributed lock DynamoDB table."
  value       = aws_dynamodb_table.failover_locks.arn
}
