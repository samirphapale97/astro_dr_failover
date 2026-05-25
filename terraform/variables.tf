# -----------------------------------------------------------------------------
# Project & Environment
# -----------------------------------------------------------------------------

variable "project" {
  description = "Project name used for resource naming and tagging."
  type        = string
  default     = "airflow-dr"

  validation {
    condition     = can(regex("^[a-z0-9-]+$", var.project))
    error_message = "Project name must contain only lowercase letters, numbers, and hyphens."
  }
}

variable "environment" {
  description = "Deployment environment (dev, staging, or prod)."
  type        = string
  default     = "prod"

  validation {
    condition     = contains(["dev", "staging", "prod"], var.environment)
    error_message = "Environment must be one of: dev, staging, prod."
  }
}

# -----------------------------------------------------------------------------
# Regions
# -----------------------------------------------------------------------------

variable "primary_region" {
  description = "AWS primary region for Airflow infrastructure."
  type        = string
  default     = "us-east-1"
}

variable "dr_region" {
  description = "AWS disaster recovery region for failover."
  type        = string
  default     = "us-east-2"
}

# -----------------------------------------------------------------------------
# Secrets Configuration
# -----------------------------------------------------------------------------

variable "secret_name_template" {
  description = "Template for secret names. Use %s as placeholder for the region."
  type        = string
  default     = "/airflow/config/%s/app_config"
}

variable "primary_config" {
  description = "Configuration key-value pairs stored in Secrets Manager for the primary region (e.g., databricks_url, s3_bucket, kafka_bootstrap)."
  type        = map(string)
  default     = {}
}

variable "dr_config" {
  description = "Configuration key-value pairs stored in Secrets Manager for the DR region (e.g., databricks_url, s3_bucket, kafka_bootstrap)."
  type        = map(string)
  default     = {}
}

# -----------------------------------------------------------------------------
# Health Check
# -----------------------------------------------------------------------------

variable "health_check_fqdn" {
  description = "Fully qualified domain name to health check (e.g., Databricks workspace URL)."
  type        = string
}

variable "health_check_port" {
  description = "Port for Route53 health check."
  type        = number
  default     = 443

  validation {
    condition     = var.health_check_port > 0 && var.health_check_port <= 65535
    error_message = "Health check port must be between 1 and 65535."
  }
}

variable "health_check_path" {
  description = "HTTP path for Route53 health check."
  type        = string
  default     = "/api/2.0/clusters/list"
}

variable "health_check_type" {
  description = "Type of Route53 health check (HTTP, HTTPS, TCP)."
  type        = string
  default     = "HTTPS"

  validation {
    condition     = contains(["HTTP", "HTTPS", "TCP"], var.health_check_type)
    error_message = "Health check type must be one of: HTTP, HTTPS, TCP."
  }
}

# -----------------------------------------------------------------------------
# Notifications & Integrations
# -----------------------------------------------------------------------------

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
  description = "Astronomer (Astro) deployment ID for the Airflow environment."
  type        = string
  default     = ""
}

variable "notification_email" {
  description = "Email address for SNS failover alert subscription. Leave empty to skip."
  type        = string
  default     = ""
}

# -----------------------------------------------------------------------------
# KMS & Secrets Manager Settings
# -----------------------------------------------------------------------------

variable "kms_deletion_window" {
  description = "Number of days before KMS key deletion (7-30)."
  type        = number
  default     = 10

  validation {
    condition     = var.kms_deletion_window >= 7 && var.kms_deletion_window <= 30
    error_message = "KMS deletion window must be between 7 and 30 days."
  }
}

variable "secret_recovery_window" {
  description = "Number of days before Secrets Manager secret deletion (7-30)."
  type        = number
  default     = 7

  validation {
    condition     = var.secret_recovery_window >= 7 && var.secret_recovery_window <= 30
    error_message = "Secret recovery window must be between 7 and 30 days."
  }
}

# -----------------------------------------------------------------------------
# Tags
# -----------------------------------------------------------------------------

variable "tags" {
  description = "Additional tags to apply to all resources."
  type        = map(string)
  default     = {}
}
