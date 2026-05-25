"""Unit tests for RegionHealthChecker."""

from __future__ import annotations

import json

import pytest
from moto import mock_aws

from astro_dr.aws_client import AWSClientFactory
from astro_dr.config import DRConfig
from astro_dr.exceptions import RegionUnhealthyError
from astro_dr.health_checker import HealthStatus, RegionHealthChecker


@pytest.fixture()
def config() -> DRConfig:
    return DRConfig(
        primary_region="us-east-1",
        dr_region="us-east-2",
        health_check_timeout_seconds=5,
    )


class TestRegionHealthChecker:
    @mock_aws
    def test_healthy_region_returns_healthy(self, config):
        factory = AWSClientFactory()
        checker = RegionHealthChecker(factory, config)

        health = checker.check_region("us-east-1")
        assert health.is_healthy is True
        assert health.region == "us-east-1"
        assert health.checks["secrets_manager"] is True
        assert health.checks["ssm"] is True

    @mock_aws
    def test_check_secrets_manager_healthy(self, config):
        factory = AWSClientFactory()
        checker = RegionHealthChecker(factory, config)

        status = checker.check_secrets_manager("us-east-1")
        assert status == HealthStatus.HEALTHY

    def test_unhealthy_region_returns_unhealthy(self, config):
        """Without moto, AWS calls will fail → unhealthy."""
        from unittest.mock import MagicMock

        factory = AWSClientFactory()
        # Replace with a mock that raises
        mock_client = MagicMock()
        mock_client.list_secrets.side_effect = Exception("connection refused")
        mock_client.describe_parameters.side_effect = Exception("connection refused")
        factory._clients[("secretsmanager", "us-east-1")] = mock_client
        factory._clients[("ssm", "us-east-1")] = mock_client

        checker = RegionHealthChecker(factory, config)
        health = checker.check_region("us-east-1")

        assert health.is_healthy is False
        assert health.checks["secrets_manager"] is False
        assert health.checks["ssm"] is False
        assert health.error_message != ""

    @mock_aws
    def test_determine_active_region_primary_healthy(self, config):
        factory = AWSClientFactory()
        checker = RegionHealthChecker(factory, config)

        result = checker.determine_active_region()
        assert result == "us-east-1"

    def test_determine_active_region_primary_down_fallback_dr(self, config):
        from unittest.mock import MagicMock

        factory = AWSClientFactory()

        # Primary is down
        mock_bad = MagicMock()
        mock_bad.list_secrets.side_effect = Exception("down")
        mock_bad.describe_parameters.side_effect = Exception("down")
        factory._clients[("secretsmanager", "us-east-1")] = mock_bad
        factory._clients[("ssm", "us-east-1")] = mock_bad

        # DR is up
        mock_good = MagicMock()
        mock_good.list_secrets.return_value = {"SecretList": []}
        mock_good.describe_parameters.return_value = {"Parameters": []}
        factory._clients[("secretsmanager", "us-east-2")] = mock_good
        factory._clients[("ssm", "us-east-2")] = mock_good

        checker = RegionHealthChecker(factory, config)
        result = checker.determine_active_region()
        assert result == "us-east-2"

    def test_determine_active_region_both_down_raises(self, config):
        from unittest.mock import MagicMock

        factory = AWSClientFactory()

        mock_bad = MagicMock()
        mock_bad.list_secrets.side_effect = Exception("down")
        mock_bad.describe_parameters.side_effect = Exception("down")

        for region in ("us-east-1", "us-east-2"):
            factory._clients[("secretsmanager", region)] = mock_bad
            factory._clients[("ssm", region)] = mock_bad

        checker = RegionHealthChecker(factory, config)

        with pytest.raises(RegionUnhealthyError, match="Both primary and DR"):
            checker.determine_active_region()

    @mock_aws
    def test_latency_is_measured(self, config):
        factory = AWSClientFactory()
        checker = RegionHealthChecker(factory, config)

        health = checker.check_region("us-east-1")
        assert health.latency_ms >= 0
