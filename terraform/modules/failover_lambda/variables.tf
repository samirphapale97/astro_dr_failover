variable "project" {
  description = "Project name for resource naming."
  type        = string
}

variable "environment" {
  description = "Deployment environment (dev, staging, prod)."
  type        = string
}

variable "ssm_parameter_name" {
  description = "SSM parameter name that stores the active region."
  type        = string
}

variable "primary_region" {
  description = "AWS primary region."
  type        = string
}

variable "dr_region" {
  description = "AWS disaster recovery region."
  type        = string
}

variable "slack_webhook_url" {
  description = "Slack incoming webhook URL for failover notifications."
  type        = string
  sensitive   = true
}

variable "astro_api_key" {
  description = "Astronomer (Astro) API key for updating deployment environment variables."
  type        = string
  sensitive   = true
  default     = ""
}

variable "astro_deployment_id" {
  description = "Astronomer (Astro) deployment ID."
  type        = string
  default     = ""
}

variable "astro_api_url" {
  description = "Astronomer (Astro) API base URL."
  type        = string
  default     = "https://api.astronomer.io"
}

variable "sns_topic_arn" {
  description = "ARN of the SNS topic that triggers the Lambda function."
  type        = string
}

variable "tags" {
  description = "Tags to apply to all resources."
  type        = map(string)
  default     = {}
}

variable "sqs_queue_url" {
  description = "URL of the SQS FIFO queue for failover event buffering."
  type        = string
  default     = ""
}

variable "sqs_queue_arn" {
  description = "ARN of the SQS FIFO queue for IAM permissions."
  type        = string
  default     = ""
}

variable "state_table_name" {
  description = "DynamoDB table name for failover state tracking."
  type        = string
  default     = ""
}

variable "state_table_arn" {
  description = "ARN of the DynamoDB state table for IAM permissions."
  type        = string
  default     = ""
}

variable "lock_table_name" {
  description = "DynamoDB table name for distributed locks."
  type        = string
  default     = ""
}

variable "lock_table_arn" {
  description = "ARN of the DynamoDB lock table for IAM permissions."
  type        = string
  default     = ""
}
