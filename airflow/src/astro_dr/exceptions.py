"""Custom exception hierarchy for the Astro DR Failover system.

Every exception carries structured context attributes so that callers (and
structured-logging formatters) can attach meaningful metadata without having
to parse error strings.
"""

from __future__ import annotations


class AstroDRError(Exception):
    """Base exception for all Astro DR Failover errors."""

    def __init__(self, message: str, **context: object) -> None:
        self.context = context
        super().__init__(message)

    def __str__(self) -> str:
        base = super().__str__()
        if self.context:
            ctx = ", ".join(f"{k}={v!r}" for k, v in self.context.items())
            return f"{base} [{ctx}]"
        return base


class SecretFetchError(AstroDRError):
    """Raised when a secret cannot be fetched from AWS Secrets Manager."""

    def __init__(
        self,
        message: str,
        *,
        region: str = "",
        secret_name: str = "",
        **extra: object,
    ) -> None:
        super().__init__(message, region=region, secret_name=secret_name, **extra)
        self.region = region
        self.secret_name = secret_name


class RegionUnhealthyError(AstroDRError):
    """Raised when a region fails its health checks."""

    def __init__(
        self,
        message: str,
        *,
        region: str = "",
        checks: dict | None = None,
        **extra: object,
    ) -> None:
        super().__init__(message, region=region, **extra)
        self.region = region
        self.checks = checks or {}


class AstroAPIError(AstroDRError):
    """Raised when an Astronomer API call fails."""

    def __init__(
        self,
        message: str,
        *,
        status_code: int = 0,
        response_body: str = "",
        **extra: object,
    ) -> None:
        super().__init__(message, status_code=status_code, **extra)
        self.status_code = status_code
        self.response_body = response_body


class FailoverError(AstroDRError):
    """Raised when the failover orchestration itself fails."""

    def __init__(
        self,
        message: str,
        *,
        from_region: str = "",
        to_region: str = "",
        step: str = "",
        **extra: object,
    ) -> None:
        super().__init__(
            message, from_region=from_region, to_region=to_region, step=step, **extra
        )
        self.from_region = from_region
        self.to_region = to_region
        self.step = step


class ConfigValidationError(AstroDRError):
    """Raised when DR configuration validation fails."""

    def __init__(
        self,
        message: str,
        *,
        field: str = "",
        value: object = None,
        **extra: object,
    ) -> None:
        super().__init__(message, field=field, **extra)
        self.field = field
        self.value = value


class SlackNotificationError(AstroDRError):
    """Raised when a Slack notification fails to send."""

    def __init__(
        self,
        message: str,
        *,
        webhook_url: str = "",
        status_code: int = 0,
        **extra: object,
    ) -> None:
        super().__init__(message, status_code=status_code, **extra)
        self.webhook_url = webhook_url
        self.status_code = status_code
