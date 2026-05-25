# -----------------------------------------------------------------------------
# SQS Module — FIFO Queue for Failover Event Buffering
# Ensures zero-loss switchover by gating DAG config reads during transitions.
# Includes a dead-letter queue for failed message processing.
# -----------------------------------------------------------------------------

# DLQ for messages that fail processing
resource "aws_sqs_queue" "failover_events_dlq" {
  name                       = "${var.project}-${var.environment}-failover-events-dlq.fifo"
  fifo_queue                 = true
  message_retention_seconds  = 1209600 # 14 days
  visibility_timeout_seconds = 300

  tags = merge(var.tags, {
    Name        = "${var.project}-${var.environment}-failover-events-dlq"
    Project     = var.project
    Environment = var.environment
    ManagedBy   = "Terraform"
  })
}

# Main FIFO queue for failover events
resource "aws_sqs_queue" "failover_events" {
  name                        = "${var.project}-${var.environment}-failover-events.fifo"
  fifo_queue                  = true
  content_based_deduplication = false
  deduplication_scope         = "messageGroup"
  fifo_throughput_limit       = "perMessageGroupId"
  message_retention_seconds   = 86400 # 1 day
  visibility_timeout_seconds  = 30
  receive_wait_time_seconds   = 5 # long polling

  redrive_policy = jsonencode({
    deadLetterTargetArn = aws_sqs_queue.failover_events_dlq.arn
    maxReceiveCount     = 3
  })

  tags = merge(var.tags, {
    Name        = "${var.project}-${var.environment}-failover-events"
    Project     = var.project
    Environment = var.environment
    ManagedBy   = "Terraform"
  })
}

# Queue policy — allow Lambda and Airflow task role to send/receive
resource "aws_sqs_queue_policy" "failover_events" {
  queue_url = aws_sqs_queue.failover_events.id

  policy = jsonencode({
    Version = "2012-10-17"
    Id      = "${var.project}-${var.environment}-failover-sqs-policy"
    Statement = [
      {
        Sid       = "AllowLambdaAndAirflow"
        Effect    = "Allow"
        Principal = "*"
        Action = [
          "sqs:SendMessage",
          "sqs:ReceiveMessage",
          "sqs:DeleteMessage",
          "sqs:GetQueueAttributes",
          "sqs:PurgeQueue"
        ]
        Resource = aws_sqs_queue.failover_events.arn
        Condition = {
          StringEquals = {
            "aws:PrincipalAccount" = data.aws_caller_identity.current.account_id
          }
        }
      }
    ]
  })
}

# CloudWatch alarm for DLQ depth (messages that failed processing)
resource "aws_cloudwatch_metric_alarm" "dlq_depth" {
  alarm_name          = "${var.project}-${var.environment}-failover-dlq-depth"
  alarm_description   = "Alert when failover event messages end up in DLQ"
  namespace           = "AWS/SQS"
  metric_name         = "ApproximateNumberOfMessagesVisible"
  statistic           = "Maximum"
  period              = 60
  evaluation_periods  = 1
  threshold           = 1
  comparison_operator = "GreaterThanOrEqualToThreshold"
  treat_missing_data  = "notBreaching"

  dimensions = {
    QueueName = aws_sqs_queue.failover_events_dlq.name
  }

  alarm_actions = var.sns_topic_arn != "" ? [var.sns_topic_arn] : []

  tags = merge(var.tags, {
    Name        = "${var.project}-${var.environment}-failover-dlq-alarm"
    Project     = var.project
    Environment = var.environment
    ManagedBy   = "Terraform"
  })
}

data "aws_caller_identity" "current" {}
