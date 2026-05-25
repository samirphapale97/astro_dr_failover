"""Pydantic-based configuration for the Astro DR Failover system.

All tunables are loaded from environment variables with the ``ASTRO_DR_``
prefix and validated at startup via *pydantic-settings*.
"""

from __future__ import annotations

from pydantic import Field, field_validator
from pydantic_settings import BaseSettings, SettingsConfigDict


class DRConfig(BaseSettings):
    """Centralised, validated configuration for the DR failover system."""

    model_config = SettingsConfigDict(
        env_prefix="ASTRO_DR_",
        env_file=".env",
        env_file_encoding="utf-8",
        case_sensitive=False,
    )

    # ── AWS region settings ─────────────────────────────────────────────
    primary_region: str = Field(
        default="us-east-1",
        description="Primary AWS region for Airflow workloads.",
    )
    dr_region: str = Field(
        default="us-east-2",
        description="Disaster-recovery AWS region.",
    )

    # ── Secrets / SSM paths ─────────────────────────────────────────────
    secret_name_template: str = Field(
        default="/airflow/config/{region}/app_config",
        description="Python format-string for the Secrets Manager secret name.",
    )
    active_region_ssm_param: str = Field(
        default="/airflow/active_region",
        description="SSM Parameter Store key that holds the currently-active region.",
    )

    # ── Resilience tunables ─────────────────────────────────────────────
    cache_ttl_seconds: int = Field(
        default=300,
        ge=60,
        le=3600,
        description="TTL (in seconds) for locally cached config values.",
    )
    health_check_timeout_seconds: int = Field(
        default=5,
        description="Timeout in seconds for each region health-check probe.",
    )
    max_retries: int = Field(
        default=3,
        description="Maximum retry attempts for transient failures.",
    )
    retry_backoff_base: float = Field(
        default=2.0,
        description="Exponential-backoff base multiplier for retries.",
    )

    # ── Astronomer API ──────────────────────────────────────────────────
    astro_api_url: str = Field(
        default="https://api.astronomer.io",
        description="Base URL for the Astronomer platform API.",
    )
    astro_api_key: str = Field(
        default="",
        description="Bearer token for Astronomer API authentication.",
        json_schema_extra={"sensitive": True},
    )
    astro_deployment_id: str = Field(
        default="",
        description="Astronomer deployment ID to update.",
    )

    # ── Slack ───────────────────────────────────────────────────────────
    slack_webhook_url: str = Field(
        default="",
        description="Slack incoming-webhook URL for DR alerts.",
    )
    slack_channel: str = Field(
        default="#dr-failover-alerts",
        description="Slack channel override for webhook messages.",
    )

    # ── Validators ──────────────────────────────────────────────────────
    @field_validator("cache_ttl_seconds")
    @classmethod
    def _validate_cache_ttl(cls, value: int) -> int:
        if not 60 <= value <= 3600:
            raise ValueError("cache_ttl_seconds must be between 60 and 3600")
        return value
