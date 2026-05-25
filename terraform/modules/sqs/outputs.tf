output "queue_url" {
  description = "URL of the failover events FIFO queue."
  value       = aws_sqs_queue.failover_events.url
}

output "queue_arn" {
  description = "ARN of the failover events FIFO queue."
  value       = aws_sqs_queue.failover_events.arn
}

output "dlq_url" {
  description = "URL of the dead-letter queue."
  value       = aws_sqs_queue.failover_events_dlq.url
}

output "dlq_arn" {
  description = "ARN of the dead-letter queue."
  value       = aws_sqs_queue.failover_events_dlq.arn
}
