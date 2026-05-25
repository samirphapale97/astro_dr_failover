"""Manual DR failover DAG.

Allows operators to trigger a controlled failover to a chosen target region
through the Airflow UI with a ``target_region`` parameter.

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


def validate_target(**context):
    """Ensure the requested target region differs from the active one."""
    from astro_dr.variable_switcher import VariableSwitcher

    target = context["params"]["target_region"]
    config = _get_config()
    switcher = VariableSwitcher(config)
    active = switcher.get_active_region()

    context["ti"].xcom_push(key="active_region", value=active)
    context["ti"].xcom_push(key="target_region", value=target)

    if active == target:
        raise ValueError(
            f"Target region ({target}) is already the active region. "
            "No failover needed."
        )
    logger.info("Validated: current=%s  target=%s", active, target)


def health_check_target(**context):
    """Verify the target region is healthy before proceeding."""
    from astro_dr.aws_client import AWSClientFactory
    from astro_dr.health_checker import RegionHealthChecker

    target = context["ti"].xcom_pull(task_ids="validate_target", key="target_region")
    config = _get_config()

    checker = RegionHealthChecker(AWSClientFactory(), config)
    health = checker.check_region(target)

    if not health.is_healthy:
        raise RuntimeError(
            f"Target region {target} is NOT healthy: {health.error_message}"
        )

    context["ti"].xcom_push(key="target_latency_ms", value=health.latency_ms)
    logger.info("Target %s is healthy (latency %.1fms)", target, health.latency_ms)


def execute_failover(**context):
    """Perform the failover to the target region."""
    from astro_dr.variable_switcher import VariableSwitcher

    target = context["ti"].xcom_pull(task_ids="validate_target", key="target_region")
    config = _get_config()
    switcher = VariableSwitcher(config)
    result = switcher.execute_failover(target_region=target, reason="manual")

    context["ti"].xcom_push(key="failover_result", value={
        "success": result.success,
        "from_region": result.from_region,
        "to_region": result.to_region,
        "duration_ms": result.duration_ms,
        "timestamp": result.timestamp,
        "error_message": result.error_message,
    })
    logger.info(
        "Failover result: success=%s  %s→%s  (%.1fms)",
        result.success, result.from_region, result.to_region, result.duration_ms,
    )


def validate_post_failover(**context):
    """Confirm that config now reads from the new region."""
    from astro_dr.variable_switcher import VariableSwitcher

    target = context["ti"].xcom_pull(task_ids="validate_target", key="target_region")
    config = _get_config()
    switcher = VariableSwitcher(config)

    active = switcher.get_active_region()
    assert active == target, f"Post-failover check: expected {target}, got {active}"

    region_cfg = switcher.fetch_region_config(active)
    logger.info("Post-failover config keys: %s", list(region_cfg.keys()))
    context["ti"].xcom_push(key="post_config_keys", value=list(region_cfg.keys()))


def send_notification(**context):
    """Send a Slack message about the manual failover."""
    result = context["ti"].xcom_pull(task_ids="execute_failover", key="failover_result")
    if not result:
        logger.warning("No failover result to notify about")
        return

    config = _get_config()
    if config.slack_webhook_url:
        from astro_dr.slack_notifier import SlackNotifier

        notifier = SlackNotifier(config.slack_webhook_url, config.slack_channel)
        notifier.send_failover_alert(
            from_region=result["from_region"],
            to_region=result["to_region"],
            reason="manual failover via Airflow UI",
            timestamp=result["timestamp"],
        )
    logger.info("Notification sent for manual failover")


with DAG(
    dag_id="dr_manual_failover",
    default_args=default_args,
    description="Manually trigger a DR failover to a specified target region",
    schedule=None,
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=["dr", "failover", "manual"],
    params={
        "target_region": {
            "type": "string",
            "enum": ["us-east-1", "us-east-2"],
            "description": "AWS region to fail over to",
        },
    },
) as dag:
    t_validate = PythonOperator(
        task_id="validate_target",
        python_callable=validate_target,
    )
    t_health = PythonOperator(
        task_id="health_check_target",
        python_callable=health_check_target,
    )
    t_failover = PythonOperator(
        task_id="execute_failover",
        python_callable=execute_failover,
    )
    t_post = PythonOperator(
        task_id="validate_post_failover",
        python_callable=validate_post_failover,
    )
    t_notify = PythonOperator(
        task_id="send_notification",
        python_callable=send_notification,
    )

    t_validate >> t_health >> t_failover >> t_post >> t_notify
