"""Automatic DR failover monitoring DAG.

Runs every 2 minutes to health-check the active region and trigger
automatic failover to the DR region when degradation is detected.

Schedule: ``*/2 * * * *`` (every 2 minutes)
"""

from __future__ import annotations

import logging
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import BranchPythonOperator, PythonOperator

logger = logging.getLogger(__name__)

# ── DAG-level defaults ──────────────────────────────────────────────────

default_args = {
    "owner": "platform-engineering",
    "depends_on_past": False,
    "email_on_failure": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=1),
}


def _get_config():
    """Build a DRConfig from Airflow Variables, falling back to env vars."""
    try:
        from airflow.models import Variable

        overrides: dict[str, str] = {}
        for key in (
            "primary_region",
            "dr_region",
            "astro_api_key",
            "astro_deployment_id",
            "slack_webhook_url",
        ):
            val = Variable.get(f"dr_{key}", default_var=None)
            if val:
                overrides[key] = val

        from astro_dr.config import DRConfig

        return DRConfig(**overrides)
    except Exception:
        from astro_dr.config import DRConfig

        return DRConfig()


# ── task callables ──────────────────────────────────────────────────────


def check_health(**context):
    """Run a health check against the active region and push result."""
    from astro_dr.aws_client import AWSClientFactory
    from astro_dr.health_checker import RegionHealthChecker
    from astro_dr.variable_switcher import VariableSwitcher

    config = _get_config()
    switcher = VariableSwitcher(config)
    active_region = switcher.get_active_region()

    aws = AWSClientFactory()
    checker = RegionHealthChecker(aws, config)
    health = checker.check_region(active_region)

    context["ti"].xcom_push(key="active_region", value=active_region)
    context["ti"].xcom_push(key="is_healthy", value=health.is_healthy)
    context["ti"].xcom_push(key="health_checks", value=health.checks)
    context["ti"].xcom_push(key="latency_ms", value=health.latency_ms)
    context["ti"].xcom_push(key="error_message", value=health.error_message)

    logger.info(
        "Health check result: region=%s healthy=%s latency=%.1fms",
        active_region,
        health.is_healthy,
        health.latency_ms,
    )
    return health.is_healthy


def evaluate_failover(**context):
    """Branch: execute failover if unhealthy, otherwise skip."""
    is_healthy = context["ti"].xcom_pull(
        task_ids="check_health", key="is_healthy"
    )
    if is_healthy:
        return "skip_failover"
    return "execute_failover"


def do_execute_failover(**context):
    """Perform an automatic failover to the DR region."""
    from astro_dr.variable_switcher import VariableSwitcher

    config = _get_config()
    switcher = VariableSwitcher(config)
    result = switcher.monitor_and_failover()

    if result:
        context["ti"].xcom_push(key="failover_result", value={
            "success": result.success,
            "from_region": result.from_region,
            "to_region": result.to_region,
            "duration_ms": result.duration_ms,
            "timestamp": result.timestamp,
        })
        logger.info(
            "Failover executed: %s → %s (%.1fms)",
            result.from_region,
            result.to_region,
            result.duration_ms,
        )
    else:
        logger.info("monitor_and_failover returned None — no action taken")


def notify_result(**context):
    """Log the final outcome of the monitoring cycle."""
    is_healthy = context["ti"].xcom_pull(task_ids="check_health", key="is_healthy")
    active = context["ti"].xcom_pull(task_ids="check_health", key="active_region")
    failover = context["ti"].xcom_pull(
        task_ids="execute_failover", key="failover_result"
    )

    if is_healthy:
        logger.info("Region %s is healthy — no action required", active)
    elif failover:
        logger.info(
            "Failover completed: %s → %s  success=%s",
            failover.get("from_region"),
            failover.get("to_region"),
            failover.get("success"),
        )
    else:
        logger.warning("Region unhealthy but no failover result recorded")


# ── DAG definition ──────────────────────────────────────────────────────

with DAG(
    dag_id="dr_failover_monitor",
    default_args=default_args,
    description="Monitors active region health and triggers automatic DR failover",
    schedule="*/2 * * * *",
    start_date=datetime(2024, 1, 1),
    catchup=False,
    max_active_runs=1,
    tags=["dr", "failover", "monitoring"],
) as dag:
    t_check = PythonOperator(
        task_id="check_health",
        python_callable=check_health,
    )

    t_branch = BranchPythonOperator(
        task_id="evaluate_failover",
        python_callable=evaluate_failover,
    )

    t_failover = PythonOperator(
        task_id="execute_failover",
        python_callable=do_execute_failover,
    )

    t_skip = EmptyOperator(task_id="skip_failover")

    t_notify = PythonOperator(
        task_id="notify_result",
        python_callable=notify_result,
        trigger_rule="none_failed_min_one_success",
    )

    t_check >> t_branch >> [t_failover, t_skip] >> t_notify
