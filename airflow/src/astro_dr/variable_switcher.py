"""Core failover orchestrator.

``VariableSwitcher`` ties every subsystem together and exposes two main
entry-points:

* ``execute_failover`` — run a full failover to a specific target region.
* ``monitor_and_failover`` — health-check the current active region and
  trigger automatic failover when it is unhealthy.
"""

from __future__ import annotations

import time
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Optional

from botocore.exceptions import ClientError

from astro_dr.astro_api import AstroAPIClient
from astro_dr.aws_client import AWSClientFactory
from astro_dr.config import DRConfig
from astro_dr.exceptions import FailoverError, RegionUnhealthyError
from astro_dr.health_checker import RegionHealthChecker
from astro_dr.logger import get_logger
from astro_dr.secrets_manager import SecretsManager
from astro_dr.slack_notifier import SlackNotifier

logger = get_logger(__name__)


@dataclass
class FailoverResult:
    """Result of a failover operation."""

    success: bool
    from_region: str
    to_region: str
    timestamp: str
    duration_ms: float
    error_message: str = ""


class VariableSwitcher:
    """Orchestrates DR failover for Astronomer Airflow deployments.

    Creates all internal dependencies from the supplied :class:`DRConfig`.
    """

    def __init__(self, config: DRConfig) -> None:
        self._config = config

        # -- dependency wiring -----------------------------------------------
        self._aws = AWSClientFactory()
        self._secrets = SecretsManager(self._aws, config)
        self._health = RegionHealthChecker(self._aws, config)

        self._astro: AstroAPIClient | None = None
        if config.astro_api_key and config.astro_deployment_id:
            self._astro = AstroAPIClient(
                api_url=config.astro_api_url,
                api_key=config.astro_api_key,
                deployment_id=config.astro_deployment_id,
            )

        self._slack: SlackNotifier | None = None
        if config.slack_webhook_url:
            self._slack = SlackNotifier(
                webhook_url=config.slack_webhook_url,
                channel=config.slack_channel,
            )

        # Simple TTL cache for active region
        self._cached_region: str | None = None
        self._cache_ts: float = 0.0

    # ── SSM-backed active region ────────────────────────────────────────

    def get_active_region(self) -> str:
        """Read the active region from SSM Parameter Store (with TTL cache).

        Falls back to ``config.primary_region`` if the parameter does not
        exist yet.
        """
        now = time.monotonic()
        if self._cached_region and (now - self._cache_ts) < self._config.cache_ttl_seconds:
            return self._cached_region

        ssm = self._aws.get_ssm_client(self._config.primary_region)
        try:
            resp = ssm.get_parameter(Name=self._config.active_region_ssm_param)
            region = resp["Parameter"]["Value"]
        except ClientError:
            logger.warning(
                "SSM parameter not found, defaulting to primary region",
                extra={"operation": "get_active_region"},
            )
            region = self._config.primary_region

        self._cached_region = region
        self._cache_ts = now
        logger.info(
            "Active region resolved",
            extra={"region": region, "operation": "get_active_region"},
        )
        return region

    def set_active_region(self, region: str) -> None:
        """Persist *region* as the active region in SSM and bust the cache."""
        ssm = self._aws.get_ssm_client(self._config.primary_region)
        ssm.put_parameter(
            Name=self._config.active_region_ssm_param,
            Value=region,
            Type="String",
            Overwrite=True,
        )
        # Invalidate cache
        self._cached_region = region
        self._cache_ts = time.monotonic()
        logger.info(
            "Active region updated in SSM",
            extra={"region": region, "operation": "set_active_region"},
        )

    # ── config fetching ─────────────────────────────────────────────────

    def fetch_region_config(self, region: str) -> dict:
        """Return the application config dict stored in Secrets Manager."""
        return self._secrets.get_secret(region)

    # ── full failover ───────────────────────────────────────────────────

    def execute_failover(
        self,
        target_region: str,
        reason: str = "manual",
    ) -> FailoverResult:
        """Run a complete failover to *target_region*.

        Steps
        -----
        1. Validate target region is healthy.
        2. Fetch new config from target region.
        3. Update SSM active-region parameter.
        4. Update Astro deployment env vars (if API key configured).
        5. Send Slack notification.
        6. Return :class:`FailoverResult`.
        """
        start = time.monotonic()
        from_region = self.get_active_region()
        ts = datetime.now(tz=timezone.utc).isoformat()

        logger.info(
            "Starting failover",
            extra={
                "region": target_region,
                "operation": "execute_failover",
                "ctx": {"from": from_region, "reason": reason},
            },
        )

        try:
            # 1. Health-check target
            health = self._health.check_region(target_region)
            if not health.is_healthy:
                raise RegionUnhealthyError(
                    f"Target region {target_region} is unhealthy",
                    region=target_region,
                    checks=health.checks,
                )

            # 2. Fetch new config
            new_config = self.fetch_region_config(target_region)

            # 3. Update SSM
            self.set_active_region(target_region)

            # 4. Optionally update Astro deployment
            if self._astro and new_config:
                try:
                    env_vars = {
                        k.upper(): str(v)
                        for k, v in new_config.items()
                    }
                    self._astro.update_deployment_variables(env_vars)
                except Exception:
                    logger.exception("Failed to update Astro deployment variables")

            # 5. Slack notification
            if self._slack:
                self._slack.send_failover_alert(
                    from_region=from_region,
                    to_region=target_region,
                    reason=reason,
                    timestamp=ts,
                )

            duration_ms = round((time.monotonic() - start) * 1000, 2)
            result = FailoverResult(
                success=True,
                from_region=from_region,
                to_region=target_region,
                timestamp=ts,
                duration_ms=duration_ms,
            )
            logger.info(
                "Failover completed successfully",
                extra={
                    "operation": "execute_failover",
                    "duration_ms": duration_ms,
                    "ctx": {"result": "success"},
                },
            )
            return result

        except RegionUnhealthyError:
            raise
        except Exception as exc:
            duration_ms = round((time.monotonic() - start) * 1000, 2)
            raise FailoverError(
                f"Failover to {target_region} failed: {exc}",
                from_region=from_region,
                to_region=target_region,
                step="execute_failover",
            ) from exc

    # ── automated monitor ───────────────────────────────────────────────

    def monitor_and_failover(self) -> Optional[FailoverResult]:
        """Health-check the active region and auto-failover if unhealthy.

        Returns
        -------
        FailoverResult | None
            A result object if failover was executed, *None* if the active
            region is healthy and no action was needed.
        """
        active = self.get_active_region()
        health = self._health.check_region(active)

        if health.is_healthy:
            logger.info(
                "Active region is healthy, no failover needed",
                extra={"region": active, "operation": "monitor_and_failover"},
            )
            return None

        # Determine DR target
        dr_region = (
            self._config.dr_region
            if active == self._config.primary_region
            else self._config.primary_region
        )

        logger.warning(
            "Active region unhealthy, initiating automatic failover",
            extra={
                "region": active,
                "operation": "monitor_and_failover",
                "ctx": {"target": dr_region},
            },
        )

        return self.execute_failover(
            target_region=dr_region,
            reason=f"automatic — {active} health check failed: {health.error_message}",
        )
