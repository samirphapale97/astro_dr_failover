variable "project" {
  description = "Project name for resource naming."
  type        = string
}

variable "environment" {
  description = "Deployment environment (dev, staging, prod)."
  type        = string
}

variable "initial_active_region" {
  description = "Initial value for the active region parameter (typically the primary region)."
  type        = string
}

variable "parameter_name" {
  description = "SSM parameter name for tracking the active region."
  type        = string
  default     = "/airflow/active_region"
}

variable "tags" {
  description = "Tags to apply to all resources."
  type        = map(string)
  default     = {}
}
