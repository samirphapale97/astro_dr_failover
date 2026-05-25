"""Unit tests for VariableSwitcher (core orchestrator)."""

from __future__ import annotations

import json
from unittest.mock import MagicMock, patch

import pytest
from moto import mock_aws

from astro_dr.aws_client import AWSClientFactory
from astro_dr.config import DRConfig
from astro_dr.exceptions import RegionUnhealthyError
from astro_dr.health_checker import RegionHealth
from astro_dr.variable_switcher import FailoverResult, VariableSwitcher


@pytest.fixture()
def config() -> DRConfig:
    return DRConfig(
        primary_region="us-east-1",
        dr_region="us-east-2",
        secret_name_template="/airflow/config/{region}/app_config",
        active_region_ssm_param="/airflow/active_region",
        cache_ttl_seconds=60,
        astro_api_key="",  # no Astro API calls
        astro_deployment_id="",
        slack_webhook_url="",  # no Slack calls
    )


@pytest.fixture()
def secret_values() -> dict[str, str]:
    return {
        "databricks_url": "https://primary.databricks.com",
        "s3_bucket": "primary-bucket",
        "kafka_bootstrap": "primary-kafka:9092",
    }


@pytest.fixture()
def dr_values() -> dict[str, str]:
    return {
        "databricks_url": "https://dr.databricks.com",
        "s3_bucket": "dr-bucket",
        "kafka_bootstrap": "dr-kafka:9092",
    }


def _setup_aws(factory: AWSClientFactory, secret_values, dr_values):
    """Create secrets and SSM param in both regions via moto."""
    for region, vals in [("us-east-1", secret_values), ("us-east-2", dr_values)]:
        sm = factory.get_secrets_client(region)
        sm.create_secret(
            Name=f"/airflow/config/{region}/app_config",
            SecretString=json.dumps(vals),
        )

    ssm = factory.get_ssm_client("us-east-1")
    ssm.put_parameter(
        Name="/airflow/active_region",
        Value="us-east-1",
        Type="String",
    )


class TestVariableSwitcher:
    @mock_aws
    def test_get_active_region(self, config, secret_values, dr_values):
        switcher = VariableSwitcher(config)
        _setup_aws(switcher._aws, secret_values, dr_values)

        region = switcher.get_active_region()
        assert region == "us-east-1"

    @mock_aws
    def test_get_active_region_default_when_missing(self, config):
        """Falls back to primary when SSM param doesn't exist."""
        switcher = VariableSwitcher(config)
        region = switcher.get_active_region()
        assert region == "us-east-1"

    @mock_aws
    def test_set_active_region(self, config, secret_values, dr_values):
        switcher = VariableSwitcher(config)
        _setup_aws(switcher._aws, secret_values, dr_values)

        switcher.set_active_region("us-east-2")

        # Bust cache to force re-read
        switcher._cached_region = None
        switcher._cache_ts = 0.0
        assert switcher.get_active_region() == "us-east-2"

    @mock_aws
    def test_fetch_region_config(self, config, secret_values, dr_values):
        switcher = VariableSwitcher(config)
        _setup_aws(switcher._aws, secret_values, dr_values)

        result = switcher.fetch_region_config("us-east-1")
        assert result == secret_values

    @mock_aws
    def test_execute_failover_success(self, config, secret_values, dr_values):
        switcher = VariableSwitcher(config)
        _setup_aws(switcher._aws, secret_values, dr_values)

        result = switcher.execute_failover("us-east-2", reason="test")

        assert result.success is True
        assert result.from_region == "us-east-1"
        assert result.to_region == "us-east-2"
        assert result.duration_ms > 0

        # Confirm SSM was updated
        switcher._cached_region = None
        switcher._cache_ts = 0.0
        assert switcher.get_active_region() == "us-east-2"

    @mock_aws
    def test_execute_failover_target_unhealthy_raises(self, config, secret_values, dr_values):
        switcher = VariableSwitcher(config)
        _setup_aws(switcher._aws, secret_values, dr_values)

        # Mock health checker to report unhealthy
        switcher._health.check_region = MagicMock(
            return_value=RegionHealth(
                region="us-east-2",
                is_healthy=False,
                latency_ms=100,
                checks={"secrets_manager": False, "ssm": False},
                error_message="all checks failed",
            )
        )

        with pytest.raises(RegionUnhealthyError):
            switcher.execute_failover("us-east-2")

    @mock_aws
    def test_monitor_and_failover_when_healthy_returns_none(
        self, config, secret_values, dr_values
    ):
        switcher = VariableSwitcher(config)
        _setup_aws(switcher._aws, secret_values, dr_values)

        result = switcher.monitor_and_failover()
        assert result is None

    @mock_aws
    def test_monitor_and_failover_when_unhealthy_triggers_failover(
        self, config, secret_values, dr_values
    ):
        switcher = VariableSwitcher(config)
        _setup_aws(switcher._aws, secret_values, dr_values)

        call_count = 0
        original_check = switcher._health.check_region

        def mock_check(region):
            nonlocal call_count
            call_count += 1
            if call_count == 1:
                # First call: active region is unhealthy
                return RegionHealth(
                    region=region,
                    is_healthy=False,
                    latency_ms=100,
                    checks={"secrets_manager": False},
                    error_message="SM down",
                )
            # Subsequent calls: target region is healthy
            return original_check(region)

        switcher._health.check_region = mock_check

        result = switcher.monitor_and_failover()

        assert result is not None
        assert result.success is True
        assert result.to_region == "us-east-2"

    @mock_aws
    def test_cache_invalidation_on_region_change(self, config, secret_values, dr_values):
        switcher = VariableSwitcher(config)
        _setup_aws(switcher._aws, secret_values, dr_values)

        # Populate cache
        region1 = switcher.get_active_region()
        assert region1 == "us-east-1"
        assert switcher._cached_region == "us-east-1"

        # Change region — should update cache immediately
        switcher.set_active_region("us-east-2")
        assert switcher._cached_region == "us-east-2"

        # get_active_region should return cached value without hitting SSM
        region2 = switcher.get_active_region()
        assert region2 == "us-east-2"
