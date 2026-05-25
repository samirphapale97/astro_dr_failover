"""Region health-checking for the DR failover system.

Probes each region's AWS service endpoints and aggregates the results into
a ``RegionHealth`` dataclass so the failover orchestrator can make informed
decisions.
"""

from __future__ import annotations

import time
from dataclasses import dataclass, field
from enum import Enum

from botocore.exceptions import ClientError, EndpointConnectionError

from astro_dr.aws_client import AWSClientFactory
from astro_dr.config import DRConfig
from astro_dr.exceptions import RegionUnhealthyError
from astro_dr.logger import get_logger

logger = get_logger(__name__)


class HealthStatus(str, Enum):
    """Enumeration of possible health-check outcomes."""

    HEALTHY = "healthy"
    UNHEALTHY = "unhealthy"
    DEGRADED = "degraded"


@dataclass
class RegionHealth:
    """Aggregated health report for a single AWS region."""

    region: str
    is_healthy: bool
    latency_ms: float
    checks: dict[str, bool] = field(default_factory=dict)
    error_message: str = ""


class RegionHealthChecker:
    """Performs lightweight health probes against AWS services in a region.

    Parameters
    ----------
    aws_factory:
        Shared :class:`AWSClientFactory` instance.
    config:
        DR configuration (provides timeout values).
    """

    def __init__(self, aws_factory: AWSClientFactory, config: DRConfig) -> None:
        self._aws = aws_factory
        self._config = config

    # ── individual probes ───────────────────────────────────────────────

    def check_secrets_manager(self, region: str) -> HealthStatus:
        """Try ``list_secrets`` (cheap operation) to verify connectivity.

        Returns :attr:`HealthStatus.HEALTHY` on success, otherwise
        :attr:`HealthStatus.UNHEALTHY`.
        """
        client = self._aws.get_secrets_client(region)
        try:
            client.list_secrets(MaxResults=1)
            return HealthStatus.HEALTHY
        except (ClientError, EndpointConnectionError, Exception) as exc:
            logger.warning(
                "Secrets Manager health check failed",
                extra={"region": region, "error": str(exc)},
            )
            return HealthStatus.UNHEALTHY

    def check_ssm(self, region: str) -> HealthStatus:
        """Try ``describe_parameters`` to verify SSM connectivity."""
        client = self._aws.get_ssm_client(region)
        try:
            client.describe_parameters(MaxResults=1)
            return HealthStatus.HEALTHY
        except (ClientError, EndpointConnectionError, Exception) as exc:
            logger.warning(
                "SSM health check failed",
                extra={"region": region, "error": str(exc)},
            )
            return HealthStatus.UNHEALTHY

    # ── aggregate check ─────────────────────────────────────────────────

    def check_region(self, region: str) -> RegionHealth:
        """Run all health probes for *region* and return an aggregate report."""
        start = time.monotonic()

        checks: dict[str, bool] = {}
        error_messages: list[str] = []

        sm_status = self.check_secrets_manager(region)
        checks["secrets_manager"] = sm_status == HealthStatus.HEALTHY

        ssm_status = self.check_ssm(region)
        checks["ssm"] = ssm_status == HealthStatus.HEALTHY

        is_healthy = all(checks.values())
        latency_ms = (time.monotonic() - start) * 1000

        if not is_healthy:
            failed = [k for k, v in checks.items() if not v]
            error_messages.append(f"Failed checks: {', '.join(failed)}")

        health = RegionHealth(
            region=region,
            is_healthy=is_healthy,
            latency_ms=round(latency_ms, 2),
            checks=checks,
            error_message="; ".join(error_messages),
        )

        logger.info(
            "Region health check completed",
            extra={
                "region": region,
                "operation": "check_region",
                "duration_ms": health.latency_ms,
                "ctx": {"is_healthy": health.is_healthy, "checks": health.checks},
            },
        )
        return health

    # ── decision helper ─────────────────────────────────────────────────

    def determine_active_region(self) -> str:
        """Return the healthiest available region.

        Checks the primary region first; falls back to the DR region if
        the primary is unhealthy.

        Raises
        ------
        RegionUnhealthyError
            If *both* regions are unhealthy.
        """
        primary = self.check_region(self._config.primary_region)
        if primary.is_healthy:
            logger.info("Primary region is healthy", extra={"region": primary.region})
            return primary.region

        logger.warning(
            "Primary region unhealthy, checking DR region",
            extra={"region": primary.region},
        )

        dr = self.check_region(self._config.dr_region)
        if dr.is_healthy:
            logger.info("DR region is healthy", extra={"region": dr.region})
            return dr.region

        raise RegionUnhealthyError(
            "Both primary and DR regions are unhealthy",
            region=f"{primary.region},{dr.region}",
            checks={"primary": primary.checks, "dr": dr.checks},
        )
