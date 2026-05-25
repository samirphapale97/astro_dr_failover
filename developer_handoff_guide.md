# Developer Handoff Guide — Astro DR Failover Testing

Hello! You are receiving two repositories that work together to provide an **ACID-compliant, automated Disaster Recovery failover system** for Astronomer (Airflow). 

Since you have AWS access, you can test the full end-to-end infrastructure deployment and failover simulation.

---

## 1. Repositories Overview

You will need to clone both of these repositories:

1. **`samir_test_variable_terraform`**
   - Contains the IaC (Infrastructure as Code) to deploy the required AWS resources: Secrets Manager, SSM Parameters, Route53 Health Checks, CloudWatch Alarms, SNS, Auto-Failover Lambda, SQS (Event Gating), and DynamoDB (State Machine & Locks).
2. **`samir_test_variable_python`**
   - Contains the core Python package (`astro_dr`), the Airflow DAGs, and the automated real-environment test scripts.

---

## 2. Precautions Taken (Security)

The repositories are already configured safely:
- The `.gitignore` in the Python repo ignores `.env` files.
- The `.gitignore` in the Terraform repo ignores `terraform.tfvars` and `*.tfstate` files.
- **Do not hardcode** any AWS credentials, Astro API keys, or Slack webhooks into the code. Always use local `.env` and `.tfvars` files.

---

## 3. Testing Procedure (Step-by-Step)

Here is exactly what you need to do on your laptop to deploy and test the system.

### Step 3.1: Configure AWS Locally
Open your terminal and ensure you are authenticated with AWS:
```bash
aws configure
# Enter your AWS Access Key, Secret Key, and set default region to us-east-1
```
Verify your identity:
```bash
aws sts get-caller-identity
```

### Step 3.2: Deploy Infrastructure (Terraform)
1. Open the Terraform repository:
   ```bash
   cd samir_test_variable_terraform
   ```
2. Copy the example variables file:
   ```bash
   cp terraform.tfvars.example terraform.tfvars
   ```
3. Edit `terraform.tfvars`. You only strictly need to provide:
   - `primary_region` and `dr_region`
   - Dummy URLs/configs for `primary_config` and `dr_config`
   - `health_check_fqdn` (a dummy domain to monitor)
   - *Optional:* `slack_webhook_url` (highly recommended to see the alerts!)
4. Deploy it:
   ```bash
   terraform init
   terraform apply
   # Review the plan and type "yes"
   ```
5. **Save the outputs!** Terraform will print URLs for the SQS queue and DynamoDB tables. Keep your terminal open.

### Step 3.3: Configure Python Project
1. Open the Python repository:
   ```bash
   cd ../samir_test_variable_python
   ```
2. Set up a virtual environment and install the package:
   ```bash
   python -m venv venv
   source venv/bin/activate  # Or `.\venv\Scripts\Activate.ps1` on Windows
   pip install -e ".[dev]"
   ```
3. Set up the environment variables:
   ```bash
   cp .env.example .env
   ```
4. Edit `.env` and fill in:
   - Your AWS region
   - Optional Slack Webhook
   - **Crucial:** Paste the `SQS_QUEUE_URL`, `STATE_TABLE_NAME`, and `LOCK_TABLE_NAME` outputs that Terraform gave you.

### Step 3.4: Run the Real Environment Test
We have built an automated script that tests the entire architecture against your live AWS account.

1. **Dry Run (Validate configuration):**
   ```bash
   python scripts/test_real_env.py --dry-run
   ```
2. **Full Execution:**
   ```bash
   python scripts/test_real_env.py
   ```

**What the script does (it will pause and ask you to press Enter before critical steps):**
- Validates AWS credentials.
- Creates real AWS Secrets and SSM parameters.
- Publishes a REAL Slack message to verify connectivity.
- Executes an **ACID Failover** (acquires DynamoDB lock, publishes SQS gating event, switches SSM, records state, publishes ungate event).
- Validates that SSM correctly points to DR.
- Executes an **Automated Failback** back to Primary.
- Cleans up and deletes the test secrets.

### Step 3.5: Clean Up
Once you are done testing, destroy the AWS resources so you don't accrue costs:
```bash
cd ../samir_test_variable_terraform
terraform destroy
# Type "yes"
```

---

## 4. How it Works (Briefly)

- Airflow DAGs use the `VariableSwitcher` to read the active region from the AWS SSM Parameter (`/airflow/active_region`).
- Before reading, they check the **SQS Buffer** (`sqs_buffer.should_pause()`). If a failover is currently in transit, the DAG blocks and waits so it doesn't read a mix of primary and DR variables.
- Failovers are orchestrated by `TransactionalFailover`, which uses **DynamoDB** as a state machine to guarantee atomic rollbacks if any step of the failover crashes mid-flight.
