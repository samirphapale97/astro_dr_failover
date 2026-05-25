# -----------------------------------------------------------------------------
# Astro Airflow DR Failover — Root Module
# Orchestrates secrets, SSM, health checks, and auto-failover Lambda across
# primary (us-east-1) and DR (us-east-2) regions.
# -----------------------------------------------------------------------------

locals {
  primary_secret_name = format(var.secret_name_template, var.primary_region)
  dr_secret_name      = format(var.secret_name_template, var.dr_region)

  common_tags = {
    Project     = var.project
    Environment = var.environment
    ManagedBy   = "Terraform"
  }
}

# -----------------------------------------------------------------------------
# 1. Primary Region Secrets
# -----------------------------------------------------------------------------

module "primary_secrets" {
  source = "./modules/secrets"

  providers = {
    aws = aws.primary
  }

  project                = var.project
  environment            = var.environment
  region                 = var.primary_region
  secret_name            = local.primary_secret_name
  secret_values          = var.primary_config
  kms_deletion_window    = var.kms_deletion_window
  secret_recovery_window = var.secret_recovery_window
  tags                   = local.common_tags
}

# -----------------------------------------------------------------------------
# 2. DR Region Secrets
# -----------------------------------------------------------------------------

module "dr_secrets" {
  source = "./modules/secrets"

  providers = {
    aws = aws.dr
  }

  project                = var.project
  environment            = var.environment
  region                 = var.dr_region
  secret_name            = local.dr_secret_name
  secret_values          = var.dr_config
  kms_deletion_window    = var.kms_deletion_window
  secret_recovery_window = var.secret_recovery_window
  tags                   = local.common_tags
}

# -----------------------------------------------------------------------------
# 3. SSM Parameter — Active Region Tracker
# -----------------------------------------------------------------------------

module "ssm" {
  source = "./modules/ssm"

  providers = {
    aws = aws.primary
  }

  project               = var.project
  environment           = var.environment
  initial_active_region = var.primary_region
  parameter_name        = "/airflow/active_region"
  tags                  = local.common_tags
}

# -----------------------------------------------------------------------------
# 4. Route53 Health Check & CloudWatch Alarm
# -----------------------------------------------------------------------------

module "health_check" {
  source = "./modules/health_check"

  # Route53 health checks are global resources, created in us-east-1
  providers = {
    aws = aws.primary
  }

  project            = var.project
  environment        = var.environment
  health_check_fqdn  = var.health_check_fqdn
  health_check_port  = var.health_check_port
  health_check_path  = var.health_check_path
  health_check_type  = var.health_check_type
  notification_email = var.notification_email
  tags               = local.common_tags
}

# -----------------------------------------------------------------------------
# 5. SQS — Failover Event Buffer (FIFO + DLQ)
# Ensures zero-loss switchover by gating DAG config reads during transitions.
# -----------------------------------------------------------------------------

module "sqs" {
  source = "./modules/sqs"

  providers = {
    aws = aws.primary
  }

  project       = var.project
  environment   = var.environment
  sns_topic_arn = module.health_check.sns_topic_arn
  tags          = local.common_tags
}

# -----------------------------------------------------------------------------
# 6. DynamoDB — Failover State Machine + Distributed Lock
# Provides ACID-like guarantees: state persistence, crash recovery, fencing.
# -----------------------------------------------------------------------------

module "dynamodb" {
  source = "./modules/dynamodb"

  providers = {
    aws = aws.primary
  }

  project     = var.project
  environment = var.environment
  tags        = local.common_tags
}

# -----------------------------------------------------------------------------
# 7. Auto-Failover Lambda
# -----------------------------------------------------------------------------

module "failover_lambda" {
  source = "./modules/failover_lambda"

  providers = {
    aws = aws.primary
  }

  project             = var.project
  environment         = var.environment
  ssm_parameter_name  = module.ssm.parameter_name
  primary_region      = var.primary_region
  dr_region           = var.dr_region
  slack_webhook_url   = var.slack_webhook_url
  astro_api_key       = var.astro_api_key
  astro_deployment_id = var.astro_deployment_id
  astro_api_url       = "https://api.astronomer.io"
  sns_topic_arn       = module.health_check.sns_topic_arn
  sqs_queue_url       = module.sqs.queue_url
  state_table_name    = module.dynamodb.state_table_name
  lock_table_name     = module.dynamodb.lock_table_name
  sqs_queue_arn       = module.sqs.queue_arn
  state_table_arn     = module.dynamodb.state_table_arn
  lock_table_arn      = module.dynamodb.lock_table_arn
  tags                = local.common_tags
}

