"""Unit tests for SecretsManager."""

from __future__ import annotations

import json

import pytest
from moto import mock_aws

from astro_dr.aws_client import AWSClientFactory
from astro_dr.config import DRConfig
from astro_dr.exceptions import SecretFetchError
from astro_dr.secrets_manager import SecretsManager


@pytest.fixture()
def config() -> DRConfig:
    return DRConfig(
        primary_region="us-east-1",
        dr_region="us-east-2",
        secret_name_template="/airflow/config/{region}/app_config",
    )


@pytest.fixture()
def secret_values() -> dict[str, str]:
    return {
        "databricks_url": "https://primary.databricks.com",
        "s3_bucket": "primary-bucket",
        "kafka_bootstrap": "primary-kafka:9092",
    }


class TestSecretsManager:
    @mock_aws
    def test_get_secret_success(self, config, secret_values):
        factory = AWSClientFactory()
        sm = SecretsManager(factory, config)

        # Create the secret first
        client = factory.get_secrets_client("us-east-1")
        client.create_secret(
            Name="/airflow/config/us-east-1/app_config",
            SecretString=json.dumps(secret_values),
        )

        result = sm.get_secret("us-east-1")
        assert result == secret_values

    @mock_aws
    def test_get_secret_not_found_raises(self, config):
        factory = AWSClientFactory()
        sm = SecretsManager(factory, config)

        with pytest.raises(SecretFetchError, match="not found|Failed"):
            sm.get_secret("us-east-1")

    @mock_aws
    def test_get_secret_retries_on_transient_error(self, config, secret_values):
        """The retry wrapper should handle transient ClientErrors."""
        factory = AWSClientFactory()
        sm = SecretsManager(factory, config)

        # Create the secret so the call eventually succeeds
        client = factory.get_secrets_client("us-east-1")
        client.create_secret(
            Name="/airflow/config/us-east-1/app_config",
            SecretString=json.dumps(secret_values),
        )

        # The call should succeed (tenacity retries transparently)
        result = sm.get_secret("us-east-1")
        assert result["s3_bucket"] == "primary-bucket"

    @mock_aws
    def test_put_secret_success(self, config, secret_values):
        factory = AWSClientFactory()
        sm = SecretsManager(factory, config)

        # Create initial secret
        client = factory.get_secrets_client("us-east-1")
        client.create_secret(
            Name="/airflow/config/us-east-1/app_config",
            SecretString=json.dumps(secret_values),
        )

        # Update it
        new_values = {**secret_values, "s3_bucket": "updated-bucket"}
        sm.put_secret("us-east-1", new_values)

        # Verify
        result = sm.get_secret("us-east-1")
        assert result["s3_bucket"] == "updated-bucket"

    def test_build_secret_name(self, config):
        factory = AWSClientFactory()
        sm = SecretsManager(factory, config)
        assert sm._build_secret_name("us-east-1") == "/airflow/config/us-east-1/app_config"
        assert sm._build_secret_name("eu-west-1") == "/airflow/config/eu-west-1/app_config"

    @mock_aws
    def test_get_secret_invalid_json_raises(self, config):
        factory = AWSClientFactory()
        sm = SecretsManager(factory, config)

        client = factory.get_secrets_client("us-east-1")
        client.create_secret(
            Name="/airflow/config/us-east-1/app_config",
            SecretString="not-valid-json{{{",
        )

        with pytest.raises(SecretFetchError, match="not valid JSON|Failed"):
            sm.get_secret("us-east-1")
