# Astro DR Failover — Python

Production-ready environment variable switcher for **Astronomer (Astro) Airflow** deployments with automatic multi-region disaster recovery.

## Problem

When running Airflow DAGs on Astronomer, during a **region failover** (e.g., `us-east-1` → `us-east-2`), the environment variables / Airflow Variables remain stale — pointing to the primary region's resources (Databricks URL, S3 bucket, Kafka brokers, etc.). This causes DAGs to fail or connect to the wrong infrastructure.

## Solution

This package provides:

- **Automatic health-check** of the active AWS region
- **Automatic failover** — switches config to DR region when primary is unhealthy
- **SSM-backed active region** — single source of truth for which region is active
- **Astro API integration** — updates Astronomer deployment env vars automatically
- **Slack notifications** — real-time alerts on failover/recovery events
- **TTL cache with forced invalidation** — no stale config after region switch
- **3 Airflow DAGs** — monitor, manual failover, and config test

## Architecture

```
┌─────────────────────────────────────────────────────────────┐
│                    Astro Airflow Deployment                  │
│                                                             │
│  ┌──────────────────┐    ┌─────────────────────────────┐    │
│  │ failover_monitor │───▶│     VariableSwitcher         │    │
│  │   DAG (*/2 min)  │    │                              │    │
│  └──────────────────┘    │  ┌─────────────────────────┐ │    │
│                          │  │   RegionHealthChecker    │ │    │
│  ┌──────────────────┐    │  └────────┬────────────────┘ │    │
│  │ manual_failover  │───▶│           │                  │    │
│  │      DAG         │    │  ┌────────▼────────────────┐ │    │
│  └──────────────────┘    │  │   SecretsManager        │ │    │
│                          │  └────────┬────────────────┘ │    │
│  ┌──────────────────┐    │           │                  │    │
│  │  config_test     │───▶│  ┌────────▼────────────────┐ │    │
│  │      DAG         │    │  │   SlackNotifier         │ │    │
│  └──────────────────┘    │  └─────────────────────────┘ │    │
│                          └─────────────────────────────────┘    │
└─────────────────────────────────────────────────────────────┘
              │                           │
    ┌─────────▼──────────┐     ┌──────────▼──────────┐
    │ AWS Secrets Manager│     │ AWS SSM Parameter    │
    │  us-east-1         │     │  /airflow/active_    │
    │  us-east-2         │     │   region             │
    └────────────────────┘     └─────────────────────┘
```

## Project Structure

```
samir_test_variable_python/
├── pyproject.toml              # Package config + dependencies
├── Makefile                    # Convenience commands
├── README.md
├── .gitignore
├── src/
│   └── astro_dr/
│       ├── __init__.py
│       ├── config.py           # Pydantic settings (DRConfig)
│       ├── exceptions.py       # Custom exception hierarchy
│       ├── logger.py           # Structured JSON logging
│       ├── aws_client.py       # Thread-safe boto3 factory
│       ├── secrets_manager.py  # Secrets Manager with retry
│       ├── health_checker.py   # Region health checking
│       ├── slack_notifier.py   # Slack webhook notifications
│       ├── astro_api.py        # Astronomer API client
│       └── variable_switcher.py # Core failover orchestrator
├── dags/
│   ├── failover_monitor_dag.py # Auto-monitoring (every 2 min)
│   ├── manual_failover_dag.py  # Manual failover trigger
│   └── config_test_dag.py      # Config validation
└── tests/
    ├── conftest.py             # Shared fixtures
    ├── unit/
    │   ├── test_config.py
    │   ├── test_aws_client.py
    │   ├── test_secrets_manager.py
    │   ├── test_health_checker.py
    │   ├── test_slack_notifier.py
    │   ├── test_astro_api.py
    │   └── test_variable_switcher.py
    └── integration/
        ├── test_failover_e2e.py
        └── test_dag_validation.py
```

## Installation

```bash
# Install the package in editable mode with dev dependencies
pip install -e ".[dev]"

# Or use Make
make install
```

## Configuration

All settings are loaded from environment variables with the `ASTRO_DR_` prefix:

| Environment Variable | Description | Default |
|---|---|---|
| `ASTRO_DR_PRIMARY_REGION` | Primary AWS region | `us-east-1` |
| `ASTRO_DR_DR_REGION` | DR AWS region | `us-east-2` |
| `ASTRO_DR_SECRET_NAME_TEMPLATE` | Secret name pattern | `/airflow/config/{region}/app_config` |
| `ASTRO_DR_ACTIVE_REGION_SSM_PARAM` | SSM parameter for active region | `/airflow/active_region` |
| `ASTRO_DR_CACHE_TTL_SECONDS` | Config cache TTL (60–3600) | `300` |
| `ASTRO_DR_MAX_RETRIES` | Retry attempts for transient failures | `3` |
| `ASTRO_DR_SLACK_WEBHOOK_URL` | Slack incoming webhook URL | (required for alerts) |
| `ASTRO_DR_SLACK_CHANNEL` | Slack channel for alerts | `#dr-failover-alerts` |
| `ASTRO_DR_ASTRO_API_KEY` | Astronomer API key | (optional) |
| `ASTRO_DR_ASTRO_DEPLOYMENT_ID` | Astronomer deployment ID | (optional) |

## Usage

### Deploy to Astro

1. Copy the `dags/` folder contents to your Astro project's `dags/` directory
2. Add `astro-dr-failover` to your `requirements.txt`
3. Set the environment variables in Astro UI or via `astro deployment variable create`
4. Deploy: `astro deploy`

### Automatic Monitoring

The `failover_monitor_dag` runs every **2 minutes**:
1. Health-checks the active region (Secrets Manager connectivity)
2. If unhealthy → automatically fails over to DR region
3. Sends Slack notification with failover details
4. If healthy → no action

### Manual Failover

Trigger the `manual_failover_dag` with parameter:
```json
{
  "target_region": "us-east-2"
}
```

### Config Validation

Run `config_test_dag` manually to verify current config:
- Reads active region from SSM
- Fetches config from Secrets Manager
- Tests connectivity to configured services

## How Failover Works

```
1. failover_monitor_dag fires (every 2 min)
       │
2. Health-check active region
       │
       ├── Healthy → No action ✅
       │
       └── Unhealthy → Start failover ⚠️
               │
3. Validate DR region is healthy
               │
4. Fetch config from DR Secrets Manager
               │
5. Update SSM Parameter (ACTIVE_REGION → DR)
               │
6. Update Astro deployment env vars (if configured)
               │
7. Invalidate local cache
               │
8. Send Slack notification 🔔
               │
9. All subsequent DAG runs read from DR ✅
```

## Testing

```bash
# Run all tests
make test

# Run with coverage
make test-cov

# Run only unit tests
pytest tests/unit/ -v

# Run only integration tests
pytest tests/integration/ -v

# Lint
make lint
```

### Test Categories

- **Unit tests** — mock all external services, test each module in isolation
- **Integration tests** — use `moto` to simulate AWS, test full failover lifecycle
- **DAG validation** — ensure DAG files parse without errors

## Slack Notifications

### Failover Alert
```
🚨 DR FAILOVER TRIGGERED
From Region:  us-east-1
To Region:    us-east-2
Trigger:      Automatic (Health Check Failed)
Timestamp:    2024-01-15 10:30:45 UTC
```

### Recovery Alert
```
✅ Region Recovery Detected
Active Region: us-east-2
⚠️ Auto-failback is disabled. Use manual DAG to switch back.
```

## Security Considerations

- API keys are loaded from environment variables (never hardcoded)
- Secrets Manager access uses IAM roles (no long-lived credentials)
- Slack webhook URLs are treated as sensitive values
- All secret values are encrypted at rest via KMS

## Troubleshooting

| Issue | Solution |
|-------|----------|
| `SecretFetchError` | Check IAM permissions for Secrets Manager in both regions |
| `RegionUnhealthyError` | Verify secrets exist in the target region |
| Slack notifications not received | Verify `SLACK_WEBHOOK_URL` and channel permissions |
| Cache serving stale config | Reduce `CACHE_TTL_SECONDS` or restart workers |
| Astro env vars not updating | Verify `ASTRO_API_KEY` has deployment admin permissions |

## License

Internal use only. © 2024
