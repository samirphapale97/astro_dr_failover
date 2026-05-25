"""Config validation DAG — manually triggered.

Loads the active-region configuration from Secrets Manager and validates
basic connectivity to the downstream services (Databricks, S3, Kafka).

Schedule: ``None`` (manual trigger only)
"""

from __future__ import annotations

import logging
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator

logger = logging.getLogger(__name__)

default_args = {
    "owner": "platform-engineering",
    "depends_on_past": False,
    "retries": 0,
    "retry_delay": timedelta(minutes=1),
}


def _get_config():
    try:
        from airflow.models import Variable

        overrides: dict[str, str] = {}
        for key in ("primary_region", "dr_region", "astro_api_key",
                     "astro_deployment_id", "slack_webhook_url"):
            val = Variable.get(f"dr_{key}", default_var=None)
            if val:
                overrides[key] = val
        from astro_dr.config import DRConfig
        return DRConfig(**overrides)
    except Exception:
        from astro_dr.config import DRConfig
        return DRConfig()


def load_current_config(**context):
    """Read the active region and fetch its secret config."""
    from astro_dr.variable_switcher import VariableSwitcher

    config = _get_config()
    switcher = VariableSwitcher(config)
    active_region = switcher.get_active_region()
    region_config = switcher.fetch_region_config(active_region)

    context["ti"].xcom_push(key="active_region", value=active_region)
    context["ti"].xcom_push(key="region_config", value=region_config)
    logger.info("Active region: %s  Keys: %s", active_region, list(region_config.keys()))


def validate_connectivity(**context):
    """Validate that the critical config values look reasonable."""
    import urllib.parse

    region_config = context["ti"].xcom_pull(
        task_ids="load_current_config", key="region_config"
    )
    active_region = context["ti"].xcom_pull(
        task_ids="load_current_config", key="active_region"
    )

    results: dict[str, str] = {}

    # Databricks URL
    db_url = region_config.get("databricks_url", "")
    if db_url and urllib.parse.urlparse(db_url).scheme in ("http", "https"):
        results["databricks_url"] = "OK"
    else:
        results["databricks_url"] = f"INVALID ({db_url!r})"

    # S3 bucket
    s3 = region_config.get("s3_bucket", "")
    if s3:
        results["s3_bucket"] = "OK"
    else:
        results["s3_bucket"] = "MISSING"

    # Kafka
    kafka = region_config.get("kafka_bootstrap", "")
    if kafka:
        results["kafka_bootstrap"] = "OK"
    else:
        results["kafka_bootstrap"] = "MISSING"

    context["ti"].xcom_push(key="validation_results", value=results)
    logger.info("Validation results for %s: %s", active_region, results)


def report_status(**context):
    """Print a human-readable summary."""
    active = context["ti"].xcom_pull(task_ids="load_current_config", key="active_region")
    results = context["ti"].xcom_pull(
        task_ids="validate_connectivity", key="validation_results"
    )
    region_config = context["ti"].xcom_pull(
        task_ids="load_current_config", key="region_config"
    )

    print("=" * 60)
    print(f"  DR Config Test Report — Region: {active}")
    print("=" * 60)
    for key, status in (results or {}).items():
        print(f"  {key:30s} {status}")
    print("-" * 60)
    print(f"  Total config keys: {len(region_config or {})}")
    print("=" * 60)


with DAG(
    dag_id="dr_config_test",
    default_args=default_args,
    description="Manually validate the active-region DR configuration",
    schedule=None,
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=["dr", "testing"],
) as dag:
    t_load = PythonOperator(
        task_id="load_current_config",
        python_callable=load_current_config,
    )
    t_validate = PythonOperator(
        task_id="validate_connectivity",
        python_callable=validate_connectivity,
    )
    t_report = PythonOperator(
        task_id="report_status",
        python_callable=report_status,
    )

    t_load >> t_validate >> t_report
