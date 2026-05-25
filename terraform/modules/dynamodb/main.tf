# -----------------------------------------------------------------------------
# DynamoDB Module — Failover State Machine + Distributed Lock Tables
#
# Two tables:
# 1. State table — tracks every failover with checkpoint data for rollback
# 2. Lock table — distributed lock to prevent concurrent failovers
#
# Both use PAY_PER_REQUEST for auto-scaling and have TTL enabled for
# automatic cleanup of old records.
# Point-in-time recovery enabled for crash recovery scenarios.
# -----------------------------------------------------------------------------

# ─── Failover State Table ───────────────────────────────────────────────

resource "aws_dynamodb_table" "failover_state" {
  name         = "${var.project}-${var.environment}-failover-state"
  billing_mode = "PAY_PER_REQUEST"
  hash_key     = "failover_id"

  attribute {
    name = "failover_id"
    type = "S"
  }

  # Auto-delete old completed failover records
  ttl {
    attribute_name = "ttl"
    enabled        = true
  }

  # Enable point-in-time recovery for crash recovery
  point_in_time_recovery {
    enabled = true
  }

  tags = merge(var.tags, {
    Name        = "${var.project}-${var.environment}-failover-state"
    Project     = var.project
    Environment = var.environment
    ManagedBy   = "Terraform"
  })
}

# ─── Distributed Lock Table ────────────────────────────────────────────

resource "aws_dynamodb_table" "failover_locks" {
  name         = "${var.project}-${var.environment}-failover-locks"
  billing_mode = "PAY_PER_REQUEST"
  hash_key     = "lock_key"

  attribute {
    name = "lock_key"
    type = "S"
  }

  # Auto-expire stale locks
  ttl {
    attribute_name = "ttl"
    enabled        = true
  }

  tags = merge(var.tags, {
    Name        = "${var.project}-${var.environment}-failover-locks"
    Project     = var.project
    Environment = var.environment
    ManagedBy   = "Terraform"
  })
}
