"""Unit tests for DRConfig (Pydantic settings model)."""

from __future__ import annotations

import pytest

from astro_dr.config import DRConfig


class TestDRConfigDefaults:
    """Verify that sensible defaults are applied."""

    def test_default_values(self):
        cfg = DRConfig()
        assert cfg.primary_region == "us-east-1"
        assert cfg.dr_region == "us-east-2"
        assert cfg.cache_ttl_seconds == 300
        assert cfg.health_check_timeout_seconds == 5
        assert cfg.max_retries == 3
        assert cfg.retry_backoff_base == 2.0
        assert cfg.astro_api_url == "https://api.astronomer.io"
        assert cfg.slack_channel == "#dr-failover-alerts"

    def test_custom_values(self):
        cfg = DRConfig(
            primary_region="eu-west-1",
            dr_region="eu-west-2",
            cache_ttl_seconds=120,
            astro_api_key="my-key",
            astro_deployment_id="dep-123",
            slack_webhook_url="https://hooks.slack.com/test",
        )
        assert cfg.primary_region == "eu-west-1"
        assert cfg.dr_region == "eu-west-2"
        assert cfg.cache_ttl_seconds == 120
        assert cfg.astro_api_key == "my-key"
        assert cfg.astro_deployment_id == "dep-123"
        assert cfg.slack_webhook_url == "https://hooks.slack.com/test"


class TestCacheTTLValidation:
    """Ensure the cache TTL validator enforces the 60-3600 range."""

    def test_cache_ttl_validation_too_low(self):
        with pytest.raises(ValueError):
            DRConfig(cache_ttl_seconds=10)

    def test_cache_ttl_validation_too_high(self):
        with pytest.raises(ValueError):
            DRConfig(cache_ttl_seconds=9999)

    def test_cache_ttl_boundary_low(self):
        cfg = DRConfig(cache_ttl_seconds=60)
        assert cfg.cache_ttl_seconds == 60

    def test_cache_ttl_boundary_high(self):
        cfg = DRConfig(cache_ttl_seconds=3600)
        assert cfg.cache_ttl_seconds == 3600


class TestSecretNameTemplate:
    def test_secret_name_template_formatting(self):
        cfg = DRConfig()
        name = cfg.secret_name_template.format(region="us-east-1")
        assert name == "/airflow/config/us-east-1/app_config"

    def test_custom_template_formatting(self):
        cfg = DRConfig(secret_name_template="/custom/{region}/secret")
        name = cfg.secret_name_template.format(region="ap-south-1")
        assert name == "/custom/ap-south-1/secret"


class TestFromEnvVars:
    def test_from_env_vars(self, monkeypatch):
        monkeypatch.setenv("ASTRO_DR_PRIMARY_REGION", "ap-southeast-1")
        monkeypatch.setenv("ASTRO_DR_DR_REGION", "ap-southeast-2")
        monkeypatch.setenv("ASTRO_DR_CACHE_TTL_SECONDS", "180")
        monkeypatch.setenv("ASTRO_DR_ASTRO_API_KEY", "env-key-123")
        monkeypatch.setenv("ASTRO_DR_ASTRO_DEPLOYMENT_ID", "env-dep-456")
        monkeypatch.setenv("ASTRO_DR_SLACK_WEBHOOK_URL", "https://hooks.slack.com/env")

        cfg = DRConfig()
        assert cfg.primary_region == "ap-southeast-1"
        assert cfg.dr_region == "ap-southeast-2"
        assert cfg.cache_ttl_seconds == 180
        assert cfg.astro_api_key == "env-key-123"
        assert cfg.astro_deployment_id == "env-dep-456"
        assert cfg.slack_webhook_url == "https://hooks.slack.com/env"
