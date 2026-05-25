"""Shared pytest fixtures for the Astro DR Failover test suite."""

from __future__ import annotations

import json
import os
from typing import Generator

import boto3
import pytest
import responses
from moto import mock_aws

from astro_dr.aws_client import AWSClientFactory
from astro_dr.config import DRConfig

PRIMARY_REGION = "us-east-1"
DR_REGION = "us-east-2"
SECRET_TEMPLATE = "/airflow/config/{region}/app_config"
SSM_PARAM = "/airflow/active_region"


# ── sample data ─────────────────────────────────────────────────────────


@pytest.fixture()
def primary_secret_values() -> dict[str, str]:
    return {
        "databricks_url": "https://primary.databricks.com",
        "s3_bucket": "primary-bucket",
        "kafka_bootstrap": "primary-kafka:9092",
    }


@pytest.fixture()
def dr_secret_values() -> dict[str, str]:
    return {
        "databricks_url": "https://dr.databricks.com",
        "s3_bucket": "dr-bucket",
        "kafka_bootstrap": "dr-kafka:9092",
    }


# ── config fixture ──────────────────────────────────────────────────────


@pytest.fixture()
def sample_config() -> DRConfig:
    """Return a ``DRConfig`` with deterministic test values."""
    return DRConfig(
        primary_region=PRIMARY_REGION,
        dr_region=DR_REGION,
        secret_name_template=SECRET_TEMPLATE,
        active_region_ssm_param=SSM_PARAM,
        cache_ttl_seconds=60,
        health_check_timeout_seconds=5,
        max_retries=3,
        retry_backoff_base=2.0,
        astro_api_url="https://api.astronomer.io",
        astro_api_key="test-api-key",
        astro_deployment_id="test-deployment-id",
        slack_webhook_url="https://hooks.slack.com/services/T00/B00/xxx",
        slack_channel="#test-alerts",
    )


# ── moto AWS mock ──────────────────────────────────────────────────────


@pytest.fixture()
def mock_aws_env() -> Generator[None, None, None]:
    """Activate moto mocks for SecretsManager and SSM in both regions."""
    with mock_aws():
        yield


@pytest.fixture()
def aws_factory(mock_aws_env) -> AWSClientFactory:
    """Return an ``AWSClientFactory`` backed by moto."""
    return AWSClientFactory()


@pytest.fixture()
def setup_secrets(
    aws_factory: AWSClientFactory,
    primary_secret_values: dict,
    dr_secret_values: dict,
) -> None:
    """Create secrets in both regions and the SSM active-region parameter."""
    for region, values in [
        (PRIMARY_REGION, primary_secret_values),
        (DR_REGION, dr_secret_values),
    ]:
        client = aws_factory.get_secrets_client(region)
        secret_name = SECRET_TEMPLATE.format(region=region)
        client.create_secret(
            Name=secret_name,
            SecretString=json.dumps(values),
        )

    # Set active region to primary
    ssm = aws_factory.get_ssm_client(PRIMARY_REGION)
    ssm.put_parameter(
        Name=SSM_PARAM,
        Value=PRIMARY_REGION,
        Type="String",
    )


# ── responses mock for Slack ────────────────────────────────────────────


@pytest.fixture()
def mock_slack_webhook() -> Generator[responses.RequestsMock, None, None]:
    """Mock the Slack webhook endpoint to return 200 OK."""
    with responses.RequestsMock() as rsps:
        rsps.add(
            responses.POST,
            "https://hooks.slack.com/services/T00/B00/xxx",
            json={"ok": True},
            status=200,
        )
        yield rsps
