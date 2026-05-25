# -----------------------------------------------------------------------------
# Terraform Backend Configuration
# Uncomment and configure for production use with S3 state storage.
# -----------------------------------------------------------------------------

# terraform {
#   backend "s3" {
#     bucket         = "your-terraform-state-bucket"
#     key            = "astro-dr-failover/terraform.tfstate"
#     region         = "us-east-1"
#     dynamodb_table = "terraform-state-lock"
#     encrypt        = true
#   }
# }
