variable "project" {
  description = "Project name for resource naming."
  type        = string
}

variable "environment" {
  description = "Deployment environment (dev, staging, prod)."
  type        = string
}

variable "region" {
  description = "AWS region where these secrets are deployed."
  type        = string
}

variable "secret_name" {
  description = "Name/path for the Secrets Manager secret."
  type        = string
}

variable "secret_values" {
  description = "Key-value pairs to store in the secret."
  type        = map(string)
  sensitive   = true
}

variable "kms_deletion_window" {
  description = "Number of days before KMS key deletion (7-30)."
  type        = number
  default     = 10
}

variable "secret_recovery_window" {
  description = "Number of days before secret deletion (7-30)."
  type        = number
  default     = 7
}

variable "tags" {
  description = "Tags to apply to all resources."
  type        = map(string)
  default     = {}
}
