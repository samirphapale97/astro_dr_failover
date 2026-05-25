variable "project" {
  description = "Project name for resource naming."
  type        = string
}

variable "environment" {
  description = "Environment name (dev/staging/prod)."
  type        = string
}

variable "sns_topic_arn" {
  description = "SNS topic ARN for DLQ alarms (optional)."
  type        = string
  default     = ""
}

variable "tags" {
  description = "Common tags to apply to all resources."
  type        = map(string)
  default     = {}
}
