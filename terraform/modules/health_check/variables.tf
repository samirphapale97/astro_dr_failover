variable "project" {
  description = "Project name for resource naming."
  type        = string
}

variable "environment" {
  description = "Deployment environment (dev, staging, prod)."
  type        = string
}

variable "health_check_fqdn" {
  description = "Fully qualified domain name to health check."
  type        = string
}

variable "health_check_port" {
  description = "Port for Route53 health check."
  type        = number
  default     = 443
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
}

variable "notification_email" {
  description = "Email address for SNS failover alert subscription. Leave empty to skip."
  type        = string
  default     = ""
}

variable "tags" {
  description = "Tags to apply to all resources."
  type        = map(string)
  default     = {}
}
