output "lambda_arn" {
  description = "ARN of the failover Lambda function."
  value       = aws_lambda_function.failover.arn
}

output "lambda_function_name" {
  description = "Name of the failover Lambda function."
  value       = aws_lambda_function.failover.function_name
}

output "lambda_role_arn" {
  description = "ARN of the IAM role used by the failover Lambda."
  value       = aws_iam_role.lambda_role.arn
}

output "log_group_name" {
  description = "Name of the CloudWatch log group for Lambda logs."
  value       = aws_cloudwatch_log_group.lambda_logs.name
}
