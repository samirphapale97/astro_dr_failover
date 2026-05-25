# -----------------------------------------------------------------------------
# SSM Module — Active Region Parameter
# Stores the currently active region. Lambda updates this on failover.
# lifecycle ignore_changes prevents Terraform from reverting Lambda-set values.
# -----------------------------------------------------------------------------

resource "aws_ssm_parameter" "active_region" {
  name        = var.parameter_name
  type        = "String"
  value       = var.initial_active_region
  description = "Active AWS region for ${var.project}-${var.environment} Airflow workloads. Updated by failover Lambda on DR events."

  tags = merge(var.tags, {
    Name        = "${var.project}-${var.environment}-active-region"
    Project     = var.project
    Environment = var.environment
    ManagedBy   = "Terraform"
  })

  lifecycle {
    ignore_changes = [value]
  }
}
