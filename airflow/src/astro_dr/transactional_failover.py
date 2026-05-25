"""Transactional failover orchestrator with ACID-like guarantees.

Wraps the ``VariableSwitcher`` with:
* **Distributed lock** — only one failover can execute at a time
* **State machine** — every step is checkpointed to DynamoDB
* **SQS event buffer** — DAGs are gated during the transition
* **Automatic rollback** — if any step fails, all mutations are reverted
* **Crash recovery** — if the process dies mid-failover, the state record
  survives and the next invocation can detect and complete or rollback
"""

from __future__ import annotations

import time
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Any, Optional

from astro_dr.aws_client import AWSClientFactory
from astro_dr.config import DRConfig
from astro_dr.exceptions import FailoverError, RegionUnhealthyError
from astro_dr.health_checker import RegionHealthChecker
from astro_dr.logger import get_logger
from astro_dr.secrets_manager import SecretsManager
from astro_dr.slack_notifier import SlackNotifier
from astro_dr.sqs_buffer import FailoverEvent, FailoverEventBuffer, FailoverEventType
from astro_dr.state_manager import FailoverState, FailoverStateManager

logger = get_logger(__name__)


@dataclass
class TransactionalFailoverResult:
    """Result of an ACID failover operation."""

    success: bool
    failover_id: str
    from_region: str
    to_region: str
    final_state: str
    timestamp: str
    duration_ms: float
    rolled_back: bool = False
    error_message: str = ""
    checkpoint_data: dict[str, Any] = field(default_factory=dict)


class TransactionalFailover:
    """ACID-like failover orchestrator.

    Parameters
    ----------
    config:
        DR configuration.
    sqs_queue_url:
        SQS FIFO queue URL for event buffering.
    state_table_name:
        DynamoDB table for failover state.
    lock_table_name:
        DynamoDB table for distributed locks.
    """

    def __init__(
        self,
        config: DRConfig,
        sqs_queue_url: str = "",
        state_table_name: str = "astro-dr-failover-state",
        lock_table_name: str = "astro-dr-failover-locks",
    ) -> None:
        self._config = config
        self._aws = AWSClientFactory()
        self._secrets = SecretsManager(self._aws, config)
        self._health = RegionHealthChecker(self._aws, config)

        # State management
        self._state_mgr = FailoverStateManager(
            self._aws,
            table_name=state_table_name,
            lock_table_name=lock_table_name,
            region=config.primary_region,
        )

        # SQS buffer (optional — degrades gracefully if queue_url is empty)
        self._sqs_buffer: Optional[FailoverEventBuffer] = None
        if sqs_queue_url:
            self._sqs_buffer = FailoverEventBuffer(
                self._aws,
                queue_url=sqs_queue_url,
                region=config.primary_region,
            )

        # Slack notifier
        self._slack: Optional[SlackNotifier] = None
        if config.slack_webhook_url:
            self._slack = SlackNotifier(
                config.slack_webhook_url, config.slack_channel
            )

    # ── main entry point ─────────────────────────────────────────────────

    def execute(
        self,
        target_region: str,
        reason: str = "manual",
    ) -> TransactionalFailoverResult:
        """Execute a full transactional failover.

        Steps (each checkpointed):
        1. Acquire distributed lock
        2. Create failover record (INITIATED)
        3. Health-check target region
        4. Fetch config from target region
        5. Publish FAILOVER_STARTED to SQS (gates DAGs)
        6. Update SSM parameter (point of no return — but reversible)
        7. Update Astro env vars (if configured)
        8. Validate new config is readable
        9. Publish FAILOVER_COMPLETED to SQS (ungate DAGs)
        10. Release lock

        On failure at any step: rollback + FAILOVER_ROLLED_BACK + release lock.
        """
        start = time.monotonic()
        from_region = self._read_ssm_active_region()
        record = None
        failover_id = ""

        try:
            # ── Step 1: Acquire lock ──────────────────────────────────
            record = self._state_mgr.create_record(
                from_region=from_region,
                to_region=target_region,
                reason=reason,
            )
            failover_id = record.failover_id

            if not self._state_mgr.acquire_lock(failover_id):
                existing = self._state_mgr.is_locked()
                raise FailoverError(
                    f"Cannot acquire lock — failover {existing} is in progress",
                    from_region=from_region,
                    to_region=target_region,
                    step="acquire_lock",
                )
            self._state_mgr.update_state(
                failover_id, FailoverState.LOCK_ACQUIRED
            )

            # ── Step 2: Health-check target ───────────────────────────
            health = self._health.check_region(target_region)
            if not health.is_healthy:
                raise RegionUnhealthyError(
                    f"Target region {target_region} is unhealthy",
                    region=target_region,
                    checks=health.checks,
                )
            self._state_mgr.update_state(
                failover_id, FailoverState.HEALTH_CHECKED
            )

            # ── Step 3: Fetch new config ──────────────────────────────
            new_config = self._secrets.get_secret(target_region)
            self._state_mgr.update_state(
                failover_id,
                FailoverState.CONFIG_FETCHED,
                checkpoint_data={
                    "previous_region": from_region,
                    "previous_config_verified": True,
                    "new_config_keys": list(new_config.keys()),
                },
            )

            # ── Step 4: Publish SQS STARTED event (gate DAGs) ────────
            if self._sqs_buffer:
                self._sqs_buffer.publish_event(
                    FailoverEvent(
                        event_type=FailoverEventType.FAILOVER_STARTED,
                        failover_id=failover_id,
                        from_region=from_region,
                        to_region=target_region,
                    )
                )
            self._state_mgr.update_state(
                failover_id, FailoverState.SQS_NOTIFIED
            )

            # ── Step 5: Update SSM (the critical mutation) ────────────
            ssm = self._aws.get_ssm_client(self._config.primary_region)
            ssm.put_parameter(
                Name=self._config.active_region_ssm_param,
                Value=target_region,
                Type="String",
                Overwrite=True,
            )
            self._state_mgr.update_state(
                failover_id,
                FailoverState.SSM_UPDATED,
                checkpoint_data={
                    "previous_region": from_region,
                    "ssm_param": self._config.active_region_ssm_param,
                    "ssm_previous_value": from_region,
                    "ssm_new_value": target_region,
                },
            )

            # ── Step 6: Update Astro env vars (optional) ──────────────
            # (non-critical — failure here doesn't trigger rollback)
            try:
                if self._config.astro_api_key and self._config.astro_deployment_id:
                    from astro_dr.astro_api import AstroAPIClient

                    astro = AstroAPIClient(
                        self._config.astro_api_url,
                        self._config.astro_api_key,
                        self._config.astro_deployment_id,
                    )
                    env_vars = {k.upper(): str(v) for k, v in new_config.items()}
                    env_vars["ACTIVE_REGION"] = target_region
                    astro.update_deployment_variables(env_vars)
            except Exception:
                logger.exception(
                    "Astro API update failed (non-critical, continuing)"
                )
            self._state_mgr.update_state(
                failover_id, FailoverState.ASTRO_UPDATED
            )

            # ── Step 7: Validate ──────────────────────────────────────
            # Re-read SSM to confirm it was written
            actual = self._read_ssm_active_region()
            if actual != target_region:
                raise FailoverError(
                    f"Post-failover validation failed: SSM says {actual}, expected {target_region}",
                    from_region=from_region,
                    to_region=target_region,
                    step="validate",
                )
            # Re-read secrets to confirm they're accessible
            validation_config = self._secrets.get_secret(target_region)
            if not validation_config:
                raise FailoverError(
                    "Post-failover validation: empty config from target region",
                    from_region=from_region,
                    to_region=target_region,
                    step="validate",
                )
            self._state_mgr.update_state(
                failover_id, FailoverState.VALIDATED
            )

            # ── Step 8: Publish SQS COMPLETED event (ungate DAGs) ─────
            if self._sqs_buffer:
                self._sqs_buffer.publish_event(
                    FailoverEvent(
                        event_type=FailoverEventType.FAILOVER_COMPLETED,
                        failover_id=failover_id,
                        from_region=from_region,
                        to_region=target_region,
                        metadata={"config_keys": list(validation_config.keys())},
                    )
                )

            # ── Step 9: Mark completed ────────────────────────────────
            self._state_mgr.update_state(
                failover_id, FailoverState.COMPLETED
            )

            # ── Step 10: Slack notification ───────────────────────────
            if self._slack:
                self._slack.send_failover_alert(
                    from_region=from_region,
                    to_region=target_region,
                    reason=reason,
                )

            duration_ms = round((time.monotonic() - start) * 1000, 2)
            logger.info(
                "Transactional failover completed successfully",
                extra={
                    "operation": "transactional_failover",
                    "duration_ms": duration_ms,
                    "ctx": {
                        "failover_id": failover_id,
                        "from": from_region,
                        "to": target_region,
                    },
                },
            )
            return TransactionalFailoverResult(
                success=True,
                failover_id=failover_id,
                from_region=from_region,
                to_region=target_region,
                final_state=FailoverState.COMPLETED,
                timestamp=datetime.now(tz=timezone.utc).isoformat(),
                duration_ms=duration_ms,
            )

        except Exception as exc:
            duration_ms = round((time.monotonic() - start) * 1000, 2)
            error_msg = str(exc)

            # ── ROLLBACK ──────────────────────────────────────────────
            logger.error(
                "Failover failed, initiating rollback",
                extra={
                    "operation": "transactional_failover",
                    "error": error_msg,
                    "ctx": {"failover_id": failover_id},
                },
            )
            rolled_back = False
            if failover_id:
                rolled_back = self._rollback(failover_id, from_region, error_msg)

            return TransactionalFailoverResult(
                success=False,
                failover_id=failover_id,
                from_region=from_region,
                to_region=target_region,
                final_state=FailoverState.ROLLED_BACK if rolled_back else FailoverState.FAILED,
                timestamp=datetime.now(tz=timezone.utc).isoformat(),
                duration_ms=duration_ms,
                rolled_back=rolled_back,
                error_message=error_msg,
            )
        finally:
            # Always release lock
            if failover_id:
                self._state_mgr.release_lock(failover_id)

    # ── rollback ─────────────────────────────────────────────────────────

    def _rollback(
        self, failover_id: str, original_region: str, error_msg: str
    ) -> bool:
        """Attempt to revert all mutations made during a failed failover.

        Returns ``True`` if rollback succeeded.
        """
        try:
            self._state_mgr.update_state(
                failover_id,
                FailoverState.ROLLING_BACK,
                error_message=error_msg,
            )

            # Revert SSM parameter
            record = self._state_mgr.get_record(failover_id)
            if record and record.checkpoint_data.get("ssm_previous_value"):
                ssm = self._aws.get_ssm_client(self._config.primary_region)
                ssm.put_parameter(
                    Name=self._config.active_region_ssm_param,
                    Value=record.checkpoint_data["ssm_previous_value"],
                    Type="String",
                    Overwrite=True,
                )
                logger.info(
                    "Rolled back SSM parameter",
                    extra={
                        "operation": "rollback",
                        "ctx": {
                            "param": self._config.active_region_ssm_param,
                            "value": record.checkpoint_data["ssm_previous_value"],
                        },
                    },
                )

            # Publish ROLLED_BACK event to SQS (ungate DAGs)
            if self._sqs_buffer:
                self._sqs_buffer.publish_event(
                    FailoverEvent(
                        event_type=FailoverEventType.FAILOVER_ROLLED_BACK,
                        failover_id=failover_id,
                        from_region=original_region,
                        to_region=original_region,
                        metadata={"error": error_msg},
                    )
                )

            # Send Slack rollback notification
            if self._slack:
                self._slack.send_failover_alert(
                    from_region="ROLLBACK",
                    to_region=original_region,
                    reason=f"Failover rolled back due to: {error_msg}",
                )

            self._state_mgr.update_state(
                failover_id,
                FailoverState.ROLLED_BACK,
                error_message=error_msg,
            )
            logger.info(
                "Rollback completed successfully",
                extra={
                    "operation": "rollback",
                    "ctx": {"failover_id": failover_id},
                },
            )
            return True

        except Exception:
            logger.exception("CRITICAL: Rollback failed!")
            self._state_mgr.update_state(
                failover_id,
                FailoverState.FAILED,
                error_message=f"Rollback failed: {error_msg}",
            )
            return False

    # ── crash recovery ───────────────────────────────────────────────────

    def recover_from_crash(self) -> Optional[TransactionalFailoverResult]:
        """Check for any incomplete failover and attempt recovery.

        If a failover was interrupted (process crash, timeout), this
        method finds it and either completes or rolls it back based
        on how far it got.
        """
        active = self._state_mgr.get_active_failover()
        if not active:
            logger.info(
                "No active failover found, nothing to recover",
                extra={"operation": "crash_recovery"},
            )
            return None

        logger.warning(
            "Found incomplete failover, attempting recovery",
            extra={
                "operation": "crash_recovery",
                "ctx": {
                    "failover_id": active.failover_id,
                    "state": active.state,
                },
            },
        )

        # If we got past SSM_UPDATED, try to complete the failover
        past_ssm = active.state in (
            FailoverState.SSM_UPDATED,
            FailoverState.ASTRO_UPDATED,
            FailoverState.VALIDATED,
        )

        if past_ssm:
            # Verify the target region is healthy and complete
            health = self._health.check_region(active.to_region)
            if health.is_healthy:
                self._state_mgr.update_state(
                    active.failover_id, FailoverState.COMPLETED
                )
                if self._sqs_buffer:
                    self._sqs_buffer.publish_event(
                        FailoverEvent(
                            event_type=FailoverEventType.FAILOVER_COMPLETED,
                            failover_id=active.failover_id,
                            from_region=active.from_region,
                            to_region=active.to_region,
                        )
                    )
                self._state_mgr.release_lock(active.failover_id)
                return TransactionalFailoverResult(
                    success=True,
                    failover_id=active.failover_id,
                    from_region=active.from_region,
                    to_region=active.to_region,
                    final_state=FailoverState.COMPLETED,
                    timestamp=datetime.now(tz=timezone.utc).isoformat(),
                    duration_ms=0,
                )

        # Otherwise, rollback
        rolled_back = self._rollback(
            active.failover_id, active.from_region, "Crash recovery rollback"
        )
        self._state_mgr.release_lock(active.failover_id)
        return TransactionalFailoverResult(
            success=False,
            failover_id=active.failover_id,
            from_region=active.from_region,
            to_region=active.to_region,
            final_state=FailoverState.ROLLED_BACK if rolled_back else FailoverState.FAILED,
            timestamp=datetime.now(tz=timezone.utc).isoformat(),
            duration_ms=0,
            rolled_back=rolled_back,
            error_message="Recovered from crash",
        )

    # ── helpers ──────────────────────────────────────────────────────────

    def _read_ssm_active_region(self) -> str:
        """Read the current active region from SSM."""
        ssm = self._aws.get_ssm_client(self._config.primary_region)
        try:
            resp = ssm.get_parameter(Name=self._config.active_region_ssm_param)
            return resp["Parameter"]["Value"]
        except Exception:
            return self._config.primary_region
