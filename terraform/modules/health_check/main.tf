# -----------------------------------------------------------------------------
# Health Check Module — Route53 Health Check, CloudWatch Alarm, SNS Topic
# Monitors the primary region endpoint and triggers alarms on failure.
# -----------------------------------------------------------------------------

data "aws_caller_identity" "current" {}

data "aws_region" "current" {}

# -----------------------------------------------------------------------------
# Route53 Health Check
# -----------------------------------------------------------------------------

resource "aws_route53_health_check" "primary" {
  fqdn              = var.health_check_fqdn
  port               = var.health_check_port
  type               = var.health_check_type
  resource_path      = var.health_check_type != "TCP" ? var.health_check_path : null
  failure_threshold  = 3
  request_interval   = 30

  tags = merge(var.tags, {
    Name        = "${var.project}-${var.environment}-primary-health-check"
    Project     = var.project
    Environment = var.environment
    ManagedBy   = "Terraform"
  })
}

# -----------------------------------------------------------------------------
# SNS Topic for Failover Alerts
# -----------------------------------------------------------------------------

resource "aws_sns_topic" "failover_alerts" {
  name = "${var.project}-${var.environment}-failover-alerts"

  tags = merge(var.tags, {
    Name        = "${var.project}-${var.environment}-failover-alerts"
    Project     = var.project
    Environment = var.environment
    ManagedBy   = "Terraform"
  })
}

resource "aws_sns_topic_policy" "default" {
  arn = aws_sns_topic.failover_alerts.arn

  policy = jsonencode({
    Version = "2012-10-17"
    Id      = "${var.project}-${var.environment}-failover-sns-policy"
    Statement = [
      {
        Sid       = "AllowCloudWatchPublish"
        Effect    = "Allow"
        Principal = {
          Service = "cloudwatch.amazonaws.com"
        }
        Action   = "SNS:Publish"
        Resource = aws_sns_topic.failover_alerts.arn
        Condition = {
          ArnLike = {
            "aws:SourceArn" = "arn:aws:cloudwatch:${data.aws_region.current.name}:${data.aws_caller_identity.current.account_id}:alarm:*"
          }
        }
      }
    ]
  })
}

resource "aws_sns_topic_subscription" "email" {
  count = var.notification_email != "" ? 1 : 0

  topic_arn = aws_sns_topic.failover_alerts.arn
  protocol  = "email"
  endpoint  = var.notification_email
}

# -----------------------------------------------------------------------------
# CloudWatch Alarm — Triggers on Health Check Failure
# Route53 health check metrics are always in us-east-1.
# -----------------------------------------------------------------------------

resource "aws_cloudwatch_metric_alarm" "primary_health" {
  alarm_name          = "${var.project}-${var.environment}-primary-health-alarm"
  alarm_description   = "Alarm when primary region health check fails for ${var.project}-${var.environment}"
  namespace           = "AWS/Route53"
  metric_name         = "HealthCheckStatus"
  statistic           = "Minimum"
  period              = 60
  evaluation_periods  = 2
  threshold           = 1
  comparison_operator = "LessThanThreshold"
  treat_missing_data  = "breaching"

  dimensions = {
    HealthCheckId = aws_route53_health_check.primary.id
  }

  alarm_actions = [aws_sns_topic.failover_alerts.arn]
  ok_actions    = [aws_sns_topic.failover_alerts.arn]

  tags = merge(var.tags, {
    Name        = "${var.project}-${var.environment}-primary-health-alarm"
    Project     = var.project
    Environment = var.environment
    ManagedBy   = "Terraform"
  })
}
