"""Unit tests for TransactionalFailover (ACID orchestrator)."""

from __future__ import annotations

import json
import uuid

import boto3
import pytest
import responses
from moto import mock_aws

from astro_dr.aws_client import AWSClientFactory
from astro_dr.config import DRConfig
from astro_dr.state_manager import FailoverState, FailoverStateManager
from astro_dr.transactional_failover import TransactionalFailover


SLACK_WEBHOOK = "https://hooks.slack.com/services/T00/B00/xxx"


def _create_infra(region: str = "us-east-1"):
    """Create all AWS resources in moto: secrets, SSM, DynamoDB, SQS."""
    # Secrets Manager
    for r, vals in [
        ("us-east-1", {"databricks_url": "https://p.db.com", "s3_bucket": "p-bucket"}),
        ("us-east-2", {"databricks_url": "https://d.db.com", "s3_bucket": "d-bucket"}),
    ]:
        sm = boto3.client("secretsmanager", region_name=r)
        sm.create_secret(
            Name=f"/airflow/config/{r}/app_config",
            SecretString=json.dumps(vals),
        )

    # SSM
    ssm = boto3.client("ssm", region_name=region)
    ssm.put_parameter(
        Name="/airflow/active_region", Value="us-east-1", Type="String"
    )

    # DynamoDB
    ddb = boto3.client("dynamodb", region_name=region)
    ddb.create_table(
        TableName="test-state",
        KeySchema=[{"AttributeName": "failover_id", "KeyType": "HASH"}],
        AttributeDefinitions=[
            {"AttributeName": "failover_id", "AttributeType": "S"}
        ],
        BillingMode="PAY_PER_REQUEST",
    )
    ddb.create_table(
        TableName="test-locks",
        KeySchema=[{"AttributeName": "lock_key", "KeyType": "HASH"}],
        AttributeDefinitions=[
            {"AttributeName": "lock_key", "AttributeType": "S"}
        ],
        BillingMode="PAY_PER_REQUEST",
    )

    # SQS FIFO
    sqs = boto3.client("sqs", region_name=region)
    resp = sqs.create_queue(
        QueueName="failover.fifo",
        Attributes={"FifoQueue": "true", "ContentBasedDeduplication": "false"},
    )
    return resp["QueueUrl"]


def _make_config(**overrides) -> DRConfig:
    defaults = dict(
        primary_region="us-east-1",
        dr_region="us-east-2",
        secret_name_template="/airflow/config/{region}/app_config",
        active_region_ssm_param="/airflow/active_region",
        cache_ttl_seconds=60,
        max_retries=1,
        retry_backoff_base=0.1,
        astro_api_key="",
        astro_deployment_id="",
        slack_webhook_url=SLACK_WEBHOOK,
        slack_channel="#test",
    )
    defaults.update(overrides)
    return DRConfig(**defaults)


class TestTransactionalFailoverSuccess:
    @mock_aws
    @responses.activate
    def test_full_transactional_failover(self):
        """Happy path: full ACID failover with state tracking."""
        responses.add(responses.POST, SLACK_WEBHOOK, json={"ok": True}, status=200)
        queue_url = _create_infra()
        config = _make_config()

        txn = TransactionalFailover(
            config=config,
            sqs_queue_url=queue_url,
            state_table_name="test-state",
            lock_table_name="test-locks",
        )

        result = txn.execute(target_region="us-east-2", reason="test")

        assert result.success is True
        assert result.from_region == "us-east-1"
        assert result.to_region == "us-east-2"
        assert result.final_state == FailoverState.COMPLETED
        assert result.failover_id  # non-empty UUID
        assert result.duration_ms > 0
        assert result.rolled_back is False

    @mock_aws
    @responses.activate
    def test_ssm_updated_after_failover(self):
        """Verify SSM parameter is switched to DR region."""
        responses.add(responses.POST, SLACK_WEBHOOK, json={"ok": True}, status=200)
        queue_url = _create_infra()
        config = _make_config()

        txn = TransactionalFailover(
            config=config,
            sqs_queue_url=queue_url,
            state_table_name="test-state",
            lock_table_name="test-locks",
        )
        txn.execute(target_region="us-east-2", reason="test")

        ssm = boto3.client("ssm", region_name="us-east-1")
        resp = ssm.get_parameter(Name="/airflow/active_region")
        assert resp["Parameter"]["Value"] == "us-east-2"

    @mock_aws
    @responses.activate
    def test_state_record_persisted(self):
        """Verify DynamoDB state record exists after completion."""
        responses.add(responses.POST, SLACK_WEBHOOK, json={"ok": True}, status=200)
        queue_url = _create_infra()
        config = _make_config()

        txn = TransactionalFailover(
            config=config,
            sqs_queue_url=queue_url,
            state_table_name="test-state",
            lock_table_name="test-locks",
        )
        result = txn.execute(target_region="us-east-2", reason="test")

        # Read state from DynamoDB
        factory = AWSClientFactory()
        mgr = FailoverStateManager(
            factory, table_name="test-state", lock_table_name="test-locks"
        )
        record = mgr.get_record(result.failover_id)
        assert record is not None
        assert record.state == FailoverState.COMPLETED
        assert record.from_region == "us-east-1"
        assert record.to_region == "us-east-2"


class TestTransactionalFailoverRollback:
    @mock_aws
    @responses.activate
    def test_rollback_on_unhealthy_target(self):
        """If target region has no secrets, failover should fail + rollback."""
        responses.add(responses.POST, SLACK_WEBHOOK, json={"ok": True}, status=200)

        # Only create primary secrets — no DR secrets → target unhealthy
        sm = boto3.client("secretsmanager", region_name="us-east-1")
        sm.create_secret(
            Name="/airflow/config/us-east-1/app_config",
            SecretString=json.dumps({"db": "primary"}),
        )
        ssm = boto3.client("ssm", region_name="us-east-1")
        ssm.put_parameter(
            Name="/airflow/active_region", Value="us-east-1", Type="String"
        )
        ddb = boto3.client("dynamodb", region_name="us-east-1")
        ddb.create_table(
            TableName="test-state",
            KeySchema=[{"AttributeName": "failover_id", "KeyType": "HASH"}],
            AttributeDefinitions=[
                {"AttributeName": "failover_id", "AttributeType": "S"}
            ],
            BillingMode="PAY_PER_REQUEST",
        )
        ddb.create_table(
            TableName="test-locks",
            KeySchema=[{"AttributeName": "lock_key", "KeyType": "HASH"}],
            AttributeDefinitions=[
                {"AttributeName": "lock_key", "AttributeType": "S"}
            ],
            BillingMode="PAY_PER_REQUEST",
        )

        config = _make_config()
        txn = TransactionalFailover(
            config=config,
            sqs_queue_url="",
            state_table_name="test-state",
            lock_table_name="test-locks",
        )

        result = txn.execute(target_region="us-east-2", reason="test-rollback")

        assert result.success is False
        assert result.error_message  # non-empty
        # SSM should still point to primary
        resp = ssm.get_parameter(Name="/airflow/active_region")
        assert resp["Parameter"]["Value"] == "us-east-1"


class TestDistributedLockPrevention:
    @mock_aws
    @responses.activate
    def test_concurrent_failover_blocked(self):
        """Second failover attempt should fail because lock is held."""
        responses.add(responses.POST, SLACK_WEBHOOK, json={"ok": True}, status=200)
        queue_url = _create_infra()
        config = _make_config()

        # Pre-acquire the lock
        factory = AWSClientFactory()
        mgr = FailoverStateManager(
            factory, table_name="test-state", lock_table_name="test-locks"
        )
        mgr.acquire_lock("existing-failover-123")

        txn = TransactionalFailover(
            config=config,
            sqs_queue_url=queue_url,
            state_table_name="test-state",
            lock_table_name="test-locks",
        )
        result = txn.execute(target_region="us-east-2", reason="blocked")

        assert result.success is False
        assert "lock" in result.error_message.lower() or "in progress" in result.error_message.lower()


class TestCrashRecovery:
    @mock_aws
    @responses.activate
    def test_recover_incomplete_failover_past_ssm(self):
        """If failover crashed after SSM update, recovery should complete it."""
        responses.add(responses.POST, SLACK_WEBHOOK, json={"ok": True}, status=200)
        queue_url = _create_infra()
        config = _make_config()

        # Simulate a crashed failover: create a record in SSM_UPDATED state
        factory = AWSClientFactory()
        mgr = FailoverStateManager(
            factory, table_name="test-state", lock_table_name="test-locks"
        )
        record = mgr.create_record("us-east-1", "us-east-2", "crash-test")
        mgr.update_state(record.failover_id, FailoverState.SSM_UPDATED)

        txn = TransactionalFailover(
            config=config,
            sqs_queue_url=queue_url,
            state_table_name="test-state",
            lock_table_name="test-locks",
        )
        result = txn.recover_from_crash()

        assert result is not None
        assert result.success is True
        assert result.final_state == FailoverState.COMPLETED

    @mock_aws
    def test_recover_no_active_returns_none(self):
        """If no active failover, recovery returns None."""
        _create_infra()
        config = _make_config()

        txn = TransactionalFailover(
            config=config,
            sqs_queue_url="",
            state_table_name="test-state",
            lock_table_name="test-locks",
        )
        result = txn.recover_from_crash()
        assert result is None
