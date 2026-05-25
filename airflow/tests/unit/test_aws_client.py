"""Unit tests for AWSClientFactory."""

from __future__ import annotations

import threading
from unittest.mock import MagicMock, patch

import pytest
from moto import mock_aws

from astro_dr.aws_client import AWSClientFactory


class TestAWSClientFactory:
    """Tests for boto3 client creation, caching, and thread safety."""

    @mock_aws
    def test_creates_secrets_client(self):
        factory = AWSClientFactory()
        client = factory.get_secrets_client("us-east-1")
        assert client is not None
        assert client.meta.service_model.service_name == "secretsmanager"

    @mock_aws
    def test_creates_ssm_client(self):
        factory = AWSClientFactory()
        client = factory.get_ssm_client("us-east-1")
        assert client is not None
        assert client.meta.service_model.service_name == "ssm"

    @mock_aws
    def test_caches_clients_per_region(self):
        factory = AWSClientFactory()
        c1 = factory.get_secrets_client("us-east-1")
        c2 = factory.get_secrets_client("us-east-1")
        assert c1 is c2

    @mock_aws
    def test_different_regions_different_clients(self):
        factory = AWSClientFactory()
        c1 = factory.get_secrets_client("us-east-1")
        c2 = factory.get_secrets_client("us-east-2")
        assert c1 is not c2

    @mock_aws
    def test_sts_client(self):
        factory = AWSClientFactory()
        client = factory.get_sts_client()
        assert client is not None
        assert client.meta.service_model.service_name == "sts"

    @mock_aws
    def test_thread_safety(self):
        """Verify concurrent access doesn't raise or corrupt the cache."""
        factory = AWSClientFactory()
        results: list[object] = []
        errors: list[Exception] = []

        def create_client(region: str) -> None:
            try:
                c = factory.get_secrets_client(region)
                results.append(c)
            except Exception as exc:
                errors.append(exc)

        threads = [
            threading.Thread(target=create_client, args=(f"us-east-{i % 2 + 1}",))
            for i in range(20)
        ]
        for t in threads:
            t.start()
        for t in threads:
            t.join()

        assert len(errors) == 0
        assert len(results) == 20
        # Should only have 2 unique clients (one per region)
        unique = {id(c) for c in results}
        assert len(unique) == 2

    @mock_aws
    def test_custom_session(self):
        import boto3

        session = boto3.Session(region_name="eu-west-1")
        factory = AWSClientFactory(session=session)
        client = factory.get_secrets_client("eu-west-1")
        assert client is not None
