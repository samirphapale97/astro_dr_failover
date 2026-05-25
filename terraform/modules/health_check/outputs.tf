output "health_check_id" {
  description = "ID of the Route53 health check."
  value       = aws_route53_health_check.primary.id
}

output "cloudwatch_alarm_arn" {
  description = "ARN of the CloudWatch metric alarm for the health check."
  value       = aws_cloudwatch_metric_alarm.primary_health.arn
}

output "sns_topic_arn" {
  description = "ARN of the SNS topic for failover alerts."
  value       = aws_sns_topic.failover_alerts.arn
}
