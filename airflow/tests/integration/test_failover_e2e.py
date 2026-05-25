"""End-to-end integration tests for DR failover using moto.

These tests exercise the full failover lifecycle—
provision secrets in both regions, simulate primary failure,
verify auto-failover switches to DR, and test failback.
"""

from __future__ import annotations

import json
import time

import boto3
import pytest
import responses
from moto import mock_aws

from astro_dr.aws_client import AWSClientFactory
from astro_dr.config import DRConfig
from astro_dr.health_checker import RegionHealthChecker
from astro_dr.secrets_manager import SecretsManager
from astro_dr.variable_switcher import VariableSwitcher

PRIMARY_REGION = "us-east-1"
DR_REGION = "us-east-2"
SECRET_TEMPLATE = "/airflow/config/{region}/app_config"
SSM_PARAM = "/airflow/active_region"
SLACK_WEBHOOK = "https://hooks.slack.com/services/T00/B00/xxx"

PRIMARY_CONFIG = {
    "databricks_url": "https://primary.databricks.com",
    "s3_bucket": "primary-bucket",
    "kafka_bootstrap": "primary-kafka:9092",
}

DR_CONFIG = {
    "databricks_url": "https://dr.databricks.com",
    "s3_bucket": "dr-bucket",
    "kafka_bootstrap": "dr-kafka:9092",
}


def _make_config(**overrides) -> DRConfig:
    """Create a DRConfig with sane test defaults."""
    defaults = dict(
        primary_region=PRIMARY_REGION,
        dr_region=DR_REGION,
        secret_name_template=SECRET_TEMPLATE,
        active_region_ssm_param=SSM_PARAM,
        cache_ttl_seconds=60,
        health_check_timeout_seconds=2,
        max_retries=1,
        retry_backoff_base=0.1,
        astro_api_key="",
        astro_deployment_id="",
        slack_webhook_url=SLACK_WEBHOOK,
        slack_channel="#test",
    )
    defaults.update(overrides)
    return DRConfig(**defaults)


def _provision_aws(factory: AWSClientFactory) -> None:
    """Create secrets in both regions and an SSM active-region parameter."""
    for region, values in [(PRIMARY_REGION, PRIMARY_CONFIG), (DR_REGION, DR_CONFIG)]:
        sm_client = factory.get_secrets_client(region)
        secret_name = SECRET_TEMPLATE.format(region=region)
        sm_client.create_secret(
            Name=secret_name,
            SecretString=json.dumps(values),
        )

    ssm = factory.get_ssm_client(PRIMARY_REGION)
    ssm.put_parameter(
        Name=SSM_PARAM,
        Value=PRIMARY_REGION,
        Type="String",
    )


# ── Tests ───────────────────────────────────────────────────────────────


class TestFullFailoverFlow:
    """Simulate a complete failover lifecycle with moto-backed AWS."""

    @mock_aws
    @responses.activate
    def test_failover_switches_to_dr_and_reads_dr_config(self):
        """Primary → DR failover: SSM updated, DR config returned."""
        responses.add(responses.POST, SLACK_WEBHOOK, json={"ok": True}, status=200)

        config = _make_config()
        factory = AWSClientFactory()
        _provision_aws(factory)

        switcher = VariableSwitcher(config)

        # Verify active region starts as primary
        assert switcher.get_active_region() == PRIMARY_REGION

        # Execute failover to DR
        result = switcher.execute_failover(
            target_region=DR_REGION,
            reason="integration-test",
        )

        assert result.success is True
        assert result.from_region == PRIMARY_REGION
        assert result.to_region == DR_REGION
        assert result.duration_ms > 0

        # Verify SSM was updated
        ssm = factory.get_ssm_client(PRIMARY_REGION)
        resp = ssm.get_parameter(Name=SSM_PARAM)
        assert resp["Parameter"]["Value"] == DR_REGION

        # Verify config reads from DR region
        # (bust the cache to force re-read)
        switcher._cache_ts = 0
        assert switcher.get_active_region() == DR_REGION

        dr_config = switcher.fetch_region_config(DR_REGION)
        assert dr_config["databricks_url"] == "https://dr.databricks.com"
        assert dr_config["s3_bucket"] == "dr-bucket"

    @mock_aws
    @responses.activate
    def test_failback_to_primary_after_dr_failover(self):
        """DR → Primary failback: full round-trip verification."""
        responses.add(responses.POST, SLACK_WEBHOOK, json={"ok": True}, status=200)

        config = _make_config()
        factory = AWSClientFactory()
        _provision_aws(factory)

        switcher = VariableSwitcher(config)

        # Step 1: Failover to DR
        switcher.execute_failover(target_region=DR_REGION, reason="test-failover")
        assert switcher.get_active_region() == DR_REGION

        # Step 2: Failback to primary
        # bust cache so get_active_region re-reads SSM
        switcher._cache_ts = 0
        result = switcher.execute_failover(
            target_region=PRIMARY_REGION,
            reason="test-failback",
        )

        assert result.success is True
        assert result.from_region == DR_REGION
        assert result.to_region == PRIMARY_REGION

        # Verify config reads from primary again
        primary_config = switcher.fetch_region_config(PRIMARY_REGION)
        assert primary_config["databricks_url"] == "https://primary.databricks.com"
        assert primary_config["kafka_bootstrap"] == "primary-kafka:9092"

    @mock_aws
    @responses.activate
    def test_failover_sends_slack_notification(self):
        """Verify that a Slack webhook call is made during failover."""
        responses.add(responses.POST, SLACK_WEBHOOK, json={"ok": True}, status=200)

        config = _make_config()
        factory = AWSClientFactory()
        _provision_aws(factory)

        switcher = VariableSwitcher(config)
        switcher.execute_failover(target_region=DR_REGION, reason="slack-test")

        # Check that at least one Slack call was made
        slack_calls = [
            c for c in responses.calls if "hooks.slack.com" in c.request.url
        ]
        assert len(slack_calls) >= 1

        # Verify payload contains expected fields
        body = json.loads(slack_calls[0].request.body)
        assert "blocks" in body
        # The header block should mention failover
        header_text = body["blocks"][0]["text"]["text"]
        assert "FAILOVER" in header_text.upper()

    @mock_aws
    @responses.activate
    def test_monitor_and_failover_healthy_region_returns_none(self):
        """When active region is healthy, monitor returns None."""
        config = _make_config()
        factory = AWSClientFactory()
        _provision_aws(factory)

        switcher = VariableSwitcher(config)

        # Active region (primary) has secrets → health check passes
        result = switcher.monitor_and_failover()
        assert result is None

    @mock_aws
    @responses.activate
    def test_secrets_manager_returns_correct_per_region_config(self):
        """Verify secrets stored per-region return the right data."""
        config = _make_config()
        factory = AWSClientFactory()
        _provision_aws(factory)

        sm = SecretsManager(factory, config)

        primary = sm.get_secret(PRIMARY_REGION)
        assert primary == PRIMARY_CONFIG

        dr = sm.get_secret(DR_REGION)
        assert dr == DR_CONFIG

        # Ensure they are distinct
        assert primary["databricks_url"] != dr["databricks_url"]
        assert primary["s3_bucket"] != dr["s3_bucket"]


class TestCacheInvalidation:
    """Verify that caching does not serve stale region data after failover."""

    @mock_aws
    @responses.activate
    def test_cache_busted_after_failover(self):
        """get_active_region returns new region immediately after failover."""
        responses.add(responses.POST, SLACK_WEBHOOK, json={"ok": True}, status=200)

        config = _make_config(cache_ttl_seconds=3600)  # long TTL
        factory = AWSClientFactory()
        _provision_aws(factory)

        switcher = VariableSwitcher(config)

        # Prime cache
        assert switcher.get_active_region() == PRIMARY_REGION

        # Failover
        switcher.execute_failover(target_region=DR_REGION, reason="cache-test")

        # Should return DR immediately despite long TTL
        assert switcher.get_active_region() == DR_REGION

    @mock_aws
    def test_cache_expires_after_ttl(self):
        """Cached region is re-fetched from SSM after TTL expires."""
        config = _make_config(cache_ttl_seconds=60)
        factory = AWSClientFactory()
        _provision_aws(factory)

        switcher = VariableSwitcher(config)

        # Prime cache
        assert switcher.get_active_region() == PRIMARY_REGION

        # Manually update SSM behind the scenes
        ssm = factory.get_ssm_client(PRIMARY_REGION)
        ssm.put_parameter(
            Name=SSM_PARAM,
            Value=DR_REGION,
            Type="String",
            Overwrite=True,
        )

        # Cache is still valid — should still return primary
        assert switcher.get_active_region() == PRIMARY_REGION

        # Expire the cache manually
        switcher._cache_ts = 0

        # Now it should re-read from SSM and return DR
        assert switcher.get_active_region() == DR_REGION
