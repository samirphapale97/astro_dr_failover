# -----------------------------------------------------------------------------
# Failover Lambda Module
# Auto-failover Lambda triggered by SNS when CloudWatch alarm fires.
# Updates SSM active region, notifies Slack, and optionally updates Astro.
# -----------------------------------------------------------------------------

# -----------------------------------------------------------------------------
# Lambda Package
# -----------------------------------------------------------------------------

data "archive_file" "lambda_zip" {
  type        = "zip"
  source_dir  = "${path.module}/src"
  output_path = "${path.module}/lambda.zip"
}

# -----------------------------------------------------------------------------
# IAM Role & Policy
# -----------------------------------------------------------------------------

resource "aws_iam_role" "lambda_role" {
  name = "${var.project}-${var.environment}-failover-lambda-role"

  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Effect = "Allow"
        Principal = {
          Service = "lambda.amazonaws.com"
        }
        Action = "sts:AssumeRole"
      }
    ]
  })

  tags = merge(var.tags, {
    Name        = "${var.project}-${var.environment}-failover-lambda-role"
    Project     = var.project
    Environment = var.environment
    ManagedBy   = "Terraform"
  })
}

resource "aws_iam_role_policy" "lambda_policy" {
  name = "${var.project}-${var.environment}-failover-lambda-policy"
  role = aws_iam_role.lambda_role.id

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Sid    = "SSMAccess"
        Effect = "Allow"
        Action = [
          "ssm:GetParameter",
          "ssm:PutParameter"
        ]
        Resource = "arn:aws:ssm:*:*:parameter${var.ssm_parameter_name}"
      },
      {
        Sid    = "SQSAccess"
        Effect = "Allow"
        Action = [
          "sqs:SendMessage",
          "sqs:ReceiveMessage",
          "sqs:DeleteMessage",
          "sqs:GetQueueAttributes"
        ]
        Resource = var.sqs_queue_arn != "" ? var.sqs_queue_arn : "*"
      },
      {
        Sid    = "DynamoDBAccess"
        Effect = "Allow"
        Action = [
          "dynamodb:GetItem",
          "dynamodb:PutItem",
          "dynamodb:UpdateItem",
          "dynamodb:DeleteItem",
          "dynamodb:Scan"
        ]
        Resource = length(compact([
          var.state_table_arn != "" ? var.state_table_arn : null,
          var.lock_table_arn != "" ? var.lock_table_arn : null
        ])) > 0 ? compact([
          var.state_table_arn != "" ? var.state_table_arn : null,
          var.lock_table_arn != "" ? var.lock_table_arn : null
        ]) : ["arn:aws:dynamodb:*:*:table/*"]
      },
      {
        Sid    = "CloudWatchLogs"
        Effect = "Allow"
        Action = [
          "logs:CreateLogGroup",
          "logs:CreateLogStream",
          "logs:PutLogEvents"
        ]
        Resource = "arn:aws:logs:*:*:log-group:/aws/lambda/${var.project}-${var.environment}-failover:*"
      }
    ]
  })
}

# -----------------------------------------------------------------------------
# Lambda Function
# -----------------------------------------------------------------------------

resource "aws_lambda_function" "failover" {
  filename         = data.archive_file.lambda_zip.output_path
  function_name    = "${var.project}-${var.environment}-failover"
  role             = aws_iam_role.lambda_role.arn
  handler          = "lambda_function.handler"
  runtime          = "python3.11"
  timeout          = 60
  memory_size      = 128
  source_code_hash = data.archive_file.lambda_zip.output_base64sha256

  environment {
    variables = {
      SSM_PARAMETER_NAME  = var.ssm_parameter_name
      PRIMARY_REGION      = var.primary_region
      DR_REGION           = var.dr_region
      SLACK_WEBHOOK_URL   = var.slack_webhook_url
      ASTRO_API_KEY       = var.astro_api_key
      ASTRO_DEPLOYMENT_ID = var.astro_deployment_id
      ASTRO_API_URL       = var.astro_api_url
      SQS_QUEUE_URL       = var.sqs_queue_url
      STATE_TABLE_NAME    = var.state_table_name
      LOCK_TABLE_NAME     = var.lock_table_name
    }
  }

  tags = merge(var.tags, {
    Name        = "${var.project}-${var.environment}-failover"
    Project     = var.project
    Environment = var.environment
    ManagedBy   = "Terraform"
  })

  depends_on = [
    aws_iam_role_policy.lambda_policy,
    aws_cloudwatch_log_group.lambda_logs,
  ]
}

# -----------------------------------------------------------------------------
# SNS Subscription & Lambda Permission
# -----------------------------------------------------------------------------

resource "aws_lambda_permission" "sns" {
  statement_id  = "AllowSNSInvoke"
  action        = "lambda:InvokeFunction"
  function_name = aws_lambda_function.failover.function_name
  principal     = "sns.amazonaws.com"
  source_arn    = var.sns_topic_arn
}

resource "aws_sns_topic_subscription" "lambda" {
  topic_arn = var.sns_topic_arn
  protocol  = "lambda"
  endpoint  = aws_lambda_function.failover.arn
}

# -----------------------------------------------------------------------------
# CloudWatch Log Group
# -----------------------------------------------------------------------------

resource "aws_cloudwatch_log_group" "lambda_logs" {
  name              = "/aws/lambda/${var.project}-${var.environment}-failover"
  retention_in_days = 14

  tags = merge(var.tags, {
    Name        = "${var.project}-${var.environment}-failover-logs"
    Project     = var.project
    Environment = var.environment
    ManagedBy   = "Terraform"
  })
}
