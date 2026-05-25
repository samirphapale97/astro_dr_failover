output "parameter_arn" {
  description = "ARN of the SSM parameter tracking the active region."
  value       = aws_ssm_parameter.active_region.arn
}

output "parameter_name" {
  description = "Name of the SSM parameter tracking the active region."
  value       = aws_ssm_parameter.active_region.name
}
