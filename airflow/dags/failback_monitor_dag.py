"""Automated failback monitoring DAG.

After a failover to the DR region, this DAG monitors the primary region's
health. Once the primary has been **healthy for a configurable stability
window** (consecutive healthy checks), it triggers an automatic failback
using the transactional failover engine (ACID guarantees).

Schedule: ``*/5 * * * *`` (every 5 minutes)

Safety features:
- **Stability window** — primary must be healthy for N consecutive checks
  (default 3 × 5 min = 15 minutes) before failback is triggered.
- **SQS gating** — DAGs are paused during the failback transition.
- **Transactional** — uses DynamoDB state machine with rollback.
- **Distributed lock** — prevents concurrent failover/failback.
- **No flapping** — if primary becomes unhealthy during the stability
  window, the counter resets.
"""

from __future__ import annotations

import logging
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import BranchPythonOperator, PythonOperator

logger = logging.getLogger(__name__)

# Number of consecutive healthy checks required before failback
STABILITY_THRESHOLD = 3

default_args = {
    "owner": "platform-engineering",
    "depends_on_past": False,
    "email_on_failure": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=2),
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


def _get_sqs_queue_url() -> str:
    """Get SQS queue URL from Airflow Variables."""
    try:
        from airflow.models import Variable

        return Variable.get("dr_sqs_queue_url", default_var="")
    except Exception:
        return ""


# ── task callables ──────────────────────────────────────────────────────


def check_current_state(**context):
    """Determine if we are currently in a failover state (DR is active)."""
    from astro_dr.variable_switcher import VariableSwitcher

    config = _get_config()
    switcher = VariableSwitcher(config)
    active_region = switcher.get_active_region()

    context["ti"].xcom_push(key="active_region", value=active_region)
    context["ti"].xcom_push(
        key="is_on_dr", value=(active_region == config.dr_region)
    )

    logger.info(
        "Current state: active_region=%s, is_on_dr=%s",
        active_region,
        active_region == config.dr_region,
    )


def evaluate_failback(**context):
    """Branch: only proceed if we are currently on DR."""
    is_on_dr = context["ti"].xcom_pull(task_ids="check_current_state", key="is_on_dr")
    if is_on_dr:
        return "check_primary_health"
    return "skip_failback"


def check_primary_health(**context):
    """Health-check the primary region."""
    from astro_dr.aws_client import AWSClientFactory
    from astro_dr.health_checker import RegionHealthChecker

    config = _get_config()
    checker = RegionHealthChecker(AWSClientFactory(), config)
    health = checker.check_region(config.primary_region)

    context["ti"].xcom_push(key="primary_healthy", value=health.is_healthy)
    context["ti"].xcom_push(key="primary_latency_ms", value=health.latency_ms)

    logger.info(
        "Primary health: healthy=%s latency=%.1fms",
        health.is_healthy,
        health.latency_ms,
    )


def evaluate_stability(**context):
    """Track consecutive healthy checks and decide if stable enough.

    Uses an Airflow Variable ``dr_primary_healthy_count`` as the counter.
    """
    from airflow.models import Variable

    primary_healthy = context["ti"].xcom_pull(
        task_ids="check_primary_health", key="primary_healthy"
    )

    current_count = int(Variable.get("dr_primary_healthy_count", default_var="0"))

    if primary_healthy:
        current_count += 1
        Variable.set("dr_primary_healthy_count", str(current_count))
        logger.info("Primary healthy count: %d/%d", current_count, STABILITY_THRESHOLD)

        if current_count >= STABILITY_THRESHOLD:
            return "execute_failback"
        return "skip_failback"
    else:
        # Reset counter — primary went unhealthy again
        Variable.set("dr_primary_healthy_count", "0")
        logger.info("Primary unhealthy — reset stability counter to 0")
        return "skip_failback"


def execute_failback(**context):
    """Execute a transactional failback to the primary region."""
    from astro_dr.transactional_failover import TransactionalFailover
    from airflow.models import Variable

    config = _get_config()
    sqs_url = _get_sqs_queue_url()

    txn = TransactionalFailover(
        config=config,
        sqs_queue_url=sqs_url,
    )

    # First check for any incomplete failovers (crash recovery)
    recovery = txn.recover_from_crash()
    if recovery:
        logger.info(
            "Crash recovery result: success=%s state=%s",
            recovery.success,
            recovery.final_state,
        )

    # Execute the failback
    result = txn.execute(
        target_region=config.primary_region,
        reason="automatic_failback_stability_check_passed",
    )

    # Reset stability counter
    Variable.set("dr_primary_healthy_count", "0")

    context["ti"].xcom_push(
        key="failback_result",
        value={
            "success": result.success,
            "failover_id": result.failover_id,
            "from_region": result.from_region,
            "to_region": result.to_region,
            "final_state": result.final_state,
            "duration_ms": result.duration_ms,
            "rolled_back": result.rolled_back,
            "error_message": result.error_message,
        },
    )

    logger.info(
        "Failback result: success=%s  %s→%s  state=%s  (%.1fms)",
        result.success,
        result.from_region,
        result.to_region,
        result.final_state,
        result.duration_ms,
    )

    if not result.success:
        raise RuntimeError(
            f"Failback failed: {result.error_message} "
            f"(rolled_back={result.rolled_back})"
        )


def notify_failback_result(**context):
    """Send Slack notification about the failback."""
    result = context["ti"].xcom_pull(
        task_ids="execute_failback", key="failback_result"
    )
    if not result:
        logger.info("No failback executed — nothing to notify")
        return

    config = _get_config()
    if config.slack_webhook_url:
        from astro_dr.slack_notifier import SlackNotifier

        notifier = SlackNotifier(config.slack_webhook_url, config.slack_channel)
        if result["success"]:
            notifier.send_recovery_alert(result["to_region"])
        else:
            notifier.send_failover_alert(
                from_region=result["from_region"],
                to_region=result["to_region"],
                reason=f"Failback failed: {result['error_message']}",
            )


# ── DAG definition ──────────────────────────────────────────────────────

with DAG(
    dag_id="dr_failback_monitor",
    default_args=default_args,
    description=(
        "Monitors primary region health after DR failover. "
        "Triggers automatic failback after stability window is met."
    ),
    schedule="*/5 * * * *",
    start_date=datetime(2024, 1, 1),
    catchup=False,
    max_active_runs=1,
    tags=["dr", "failover", "failback", "monitoring"],
) as dag:
    t_state = PythonOperator(
        task_id="check_current_state",
        python_callable=check_current_state,
    )

    t_branch_dr = BranchPythonOperator(
        task_id="evaluate_failback",
        python_callable=evaluate_failback,
    )

    t_check_primary = PythonOperator(
        task_id="check_primary_health",
        python_callable=check_primary_health,
    )

    t_branch_stability = BranchPythonOperator(
        task_id="evaluate_stability",
        python_callable=evaluate_stability,
    )

    t_failback = PythonOperator(
        task_id="execute_failback",
        python_callable=execute_failback,
    )

    t_skip = EmptyOperator(task_id="skip_failback")

    t_notify = PythonOperator(
        task_id="notify_failback_result",
        python_callable=notify_failback_result,
        trigger_rule="none_failed_min_one_success",
    )

    # Flow:
    # check_current_state
    #   └──> evaluate_failback
    #           ├──> check_primary_health
    #           │       └──> evaluate_stability
    #           │               ├──> execute_failback ──> notify
    #           │               └──> skip_failback   ──> notify
    #           └──> skip_failback ──> notify

    t_state >> t_branch_dr >> [t_check_primary, t_skip]
    t_check_primary >> t_branch_stability >> [t_failback, t_skip]
    [t_failback, t_skip] >> t_notify
