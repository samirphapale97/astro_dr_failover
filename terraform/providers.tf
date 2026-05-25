# -----------------------------------------------------------------------------
# AWS Provider Configuration
# Default provider uses primary region. Aliased providers for multi-region.
# -----------------------------------------------------------------------------

provider "aws" {
  region = var.primary_region

  default_tags {
    tags = merge(var.tags, {
      Project     = var.project
      Environment = var.environment
      ManagedBy   = "Terraform"
    })
  }
}

provider "aws" {
  alias  = "primary"
  region = var.primary_region

  default_tags {
    tags = merge(var.tags, {
      Project     = var.project
      Environment = var.environment
      Region      = var.primary_region
      ManagedBy   = "Terraform"
    })
  }
}

provider "aws" {
  alias  = "dr"
  region = var.dr_region

  default_tags {
    tags = merge(var.tags, {
      Project     = var.project
      Environment = var.environment
      Region      = var.dr_region
      ManagedBy   = "Terraform"
    })
  }
}
