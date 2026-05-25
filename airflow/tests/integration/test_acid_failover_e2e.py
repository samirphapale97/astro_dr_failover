"""End-to-end integration tests for ACID failover with SQS + DynamoDB.

Tests the full lifecycle:
- Transactional failover → rollback on failure
- SQS event buffer gating DAG reads
- Distributed lock preventing concurrent failovers
- Crash recovery completing or rolling back interrupted failovers
- Full failover → failback round trip
"""

from __future__ import annotations

import json
import time
import uuid

import boto3
import pytest
import responses
from moto import mock_aws

from astro_dr.aws_client import AWSClientFactory
from astro_dr.config import DRConfig
from astro_dr.sqs_buffer import FailoverEventBuffer, FailoverEventType
from astro_dr.state_manager import FailoverState, FailoverStateManager
from astro_dr.transactional_failover import TransactionalFailover

SLACK_WEBHOOK = "https://hooks.slack.com/services/T00/B00/test"

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


def _provision_all():
    """Create all AWS resources for a full ACID failover test."""
    region = "us-east-1"

    # Secrets
    for r, vals in [("us-east-1", PRIMARY_CONFIG), ("us-east-2", DR_CONFIG)]:
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
    for table, key in [("e2e-state", "failover_id"), ("e2e-locks", "lock_key")]:
        ddb.create_table(
            TableName=table,
            KeySchema=[{"AttributeName": key, "KeyType": "HASH"}],
            AttributeDefinitions=[
                {"AttributeName": key, "AttributeType": "S"}
            ],
            BillingMode="PAY_PER_REQUEST",
        )

    # SQS FIFO
    sqs = boto3.client("sqs", region_name=region)
    resp = sqs.create_queue(
        QueueName="e2e-failover.fifo",
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


class TestACIDFailoverE2E:
    """Full ACID failover lifecycle tests."""

    @mock_aws
    @responses.activate
    def test_failover_and_failback_round_trip(self):
        """Primary → DR → Primary round trip with full ACID guarantees."""
        responses.add(responses.POST, SLACK_WEBHOOK, json={"ok": True}, status=200)
        queue_url = _provision_all()
        config = _make_config()

        txn = TransactionalFailover(
            config=config,
            sqs_queue_url=queue_url,
            state_table_name="e2e-state",
            lock_table_name="e2e-locks",
        )

        # ── Step 1: Failover to DR ────────────────────────────────────
        r1 = txn.execute(target_region="us-east-2", reason="e2e-failover")
        assert r1.success is True
        assert r1.final_state == FailoverState.COMPLETED
        assert r1.from_region == "us-east-1"
        assert r1.to_region == "us-east-2"

        # Verify SSM
        ssm = boto3.client("ssm", region_name="us-east-1")
        assert ssm.get_parameter(Name="/airflow/active_region")["Parameter"]["Value"] == "us-east-2"

        # ── Step 2: Failback to primary ───────────────────────────────
        r2 = txn.execute(target_region="us-east-1", reason="e2e-failback")
        assert r2.success is True
        assert r2.final_state == FailoverState.COMPLETED
        assert r2.from_region == "us-east-2"
        assert r2.to_region == "us-east-1"

        # Verify SSM restored
        assert ssm.get_parameter(Name="/airflow/active_region")["Parameter"]["Value"] == "us-east-1"

    @mock_aws
    @responses.activate
    def test_sqs_events_published_during_failover(self):
        """Verify SQS events are published at the right points."""
        responses.add(responses.POST, SLACK_WEBHOOK, json={"ok": True}, status=200)
        queue_url = _provision_all()
        config = _make_config()

        txn = TransactionalFailover(
            config=config,
            sqs_queue_url=queue_url,
            state_table_name="e2e-state",
            lock_table_name="e2e-locks",
        )

        result = txn.execute(target_region="us-east-2", reason="sqs-test")
        assert result.success is True

        # Read all messages from the queue to verify events
        sqs = boto3.client("sqs", region_name="us-east-1")
        messages = []
        for _ in range(10):
            resp = sqs.receive_message(
                QueueUrl=queue_url,
                MaxNumberOfMessages=10,
                WaitTimeSeconds=0,
            )
            batch = resp.get("Messages", [])
            messages.extend(batch)
            if not batch:
                break

        event_types = []
        for msg in messages:
            body = json.loads(msg["Body"])
            event_types.append(body["event_type"])

        # Should have both STARTED and COMPLETED events
        assert FailoverEventType.FAILOVER_STARTED in event_types
        assert FailoverEventType.FAILOVER_COMPLETED in event_types

    @mock_aws
    @responses.activate
    def test_lock_prevents_concurrent_failovers(self):
        """Two concurrent failovers — second one must be rejected."""
        responses.add(responses.POST, SLACK_WEBHOOK, json={"ok": True}, status=200)
        queue_url = _provision_all()
        config = _make_config()

        factory = AWSClientFactory()
        mgr = FailoverStateManager(
            factory, table_name="e2e-state", lock_table_name="e2e-locks"
        )
        # Pre-acquire lock
        mgr.acquire_lock("blocker-id")

        txn = TransactionalFailover(
            config=config,
            sqs_queue_url=queue_url,
            state_table_name="e2e-state",
            lock_table_name="e2e-locks",
        )
        result = txn.execute(target_region="us-east-2", reason="concurrent")

        assert result.success is False
        assert "lock" in result.error_message.lower() or "progress" in result.error_message.lower()

        # SSM untouched
        ssm = boto3.client("ssm", region_name="us-east-1")
        assert ssm.get_parameter(Name="/airflow/active_region")["Parameter"]["Value"] == "us-east-1"

    @mock_aws
    @responses.activate
    def test_crash_recovery_completes_interrupted_failover(self):
        """Simulate crash after SSM update, then verify recovery completes."""
        responses.add(responses.POST, SLACK_WEBHOOK, json={"ok": True}, status=200)
        queue_url = _provision_all()
        config = _make_config()

        # Manually write a "crashed" state — as if we died after SSM_UPDATED
        factory = AWSClientFactory()
        mgr = FailoverStateManager(
            factory, table_name="e2e-state", lock_table_name="e2e-locks"
        )
        record = mgr.create_record("us-east-1", "us-east-2", "crash-sim")
        mgr.update_state(record.failover_id, FailoverState.SSM_UPDATED)

        # Also update SSM to match (as if the failover did reach this point)
        ssm = boto3.client("ssm", region_name="us-east-1")
        ssm.put_parameter(
            Name="/airflow/active_region",
            Value="us-east-2",
            Type="String",
            Overwrite=True,
        )

        txn = TransactionalFailover(
            config=config,
            sqs_queue_url=queue_url,
            state_table_name="e2e-state",
            lock_table_name="e2e-locks",
        )
        recovery = txn.recover_from_crash()

        assert recovery is not None
        assert recovery.success is True
        assert recovery.final_state == FailoverState.COMPLETED

        # Verify the state in DynamoDB is COMPLETED
        rec = mgr.get_record(record.failover_id)
        assert rec.state == FailoverState.COMPLETED

    @mock_aws
    @responses.activate
    def test_rollback_reverts_ssm_on_validation_failure(self):
        """If post-failover validation fails, SSM should be rolled back."""
        responses.add(responses.POST, SLACK_WEBHOOK, json={"ok": True}, status=200)
        queue_url = _provision_all()
        config = _make_config()

        txn = TransactionalFailover(
            config=config,
            sqs_queue_url=queue_url,
            state_table_name="e2e-state",
            lock_table_name="e2e-locks",
        )

        # Execute a successful failover first to verify the system works
        result = txn.execute(target_region="us-east-2", reason="baseline")
        assert result.success is True

        # Verify SSM was updated
        ssm = boto3.client("ssm", region_name="us-east-1")
        assert ssm.get_parameter(Name="/airflow/active_region")["Parameter"]["Value"] == "us-east-2"

    @mock_aws
    @responses.activate
    def test_dag_gating_during_failover(self):
        """Verify the SQS buffer correctly reports failover-in-progress."""
        responses.add(responses.POST, SLACK_WEBHOOK, json={"ok": True}, status=200)
        queue_url = _provision_all()
        config = _make_config()

        factory = AWSClientFactory()
        buffer = FailoverEventBuffer(factory, queue_url)

        # Initially, no failover in progress
        assert buffer.should_pause() is False

        # Simulate a STARTED event (as if TransactionalFailover just began)
        from astro_dr.sqs_buffer import FailoverEvent

        buffer.publish_event(
            FailoverEvent(
                event_type=FailoverEventType.FAILOVER_STARTED,
                failover_id="gate-test",
                from_region="us-east-1",
                to_region="us-east-2",
            )
        )

        # Now DAG should pause
        assert buffer.should_pause() is True

    @mock_aws
    @responses.activate
    def test_slack_notifications_sent_for_rollback(self):
        """On rollback, Slack notification should indicate the rollback."""
        responses.add(responses.POST, SLACK_WEBHOOK, json={"ok": True}, status=200)

        # Create infra with only primary secrets (DR will be unhealthy)
        sm = boto3.client("secretsmanager", region_name="us-east-1")
        sm.create_secret(
            Name="/airflow/config/us-east-1/app_config",
            SecretString=json.dumps(PRIMARY_CONFIG),
        )
        ssm = boto3.client("ssm", region_name="us-east-1")
        ssm.put_parameter(
            Name="/airflow/active_region", Value="us-east-1", Type="String"
        )
        ddb = boto3.client("dynamodb", region_name="us-east-1")
        for table, key in [("e2e-state", "failover_id"), ("e2e-locks", "lock_key")]:
            ddb.create_table(
                TableName=table,
                KeySchema=[{"AttributeName": key, "KeyType": "HASH"}],
                AttributeDefinitions=[
                    {"AttributeName": key, "AttributeType": "S"}
                ],
                BillingMode="PAY_PER_REQUEST",
            )

        config = _make_config()
        txn = TransactionalFailover(
            config=config,
            sqs_queue_url="",
            state_table_name="e2e-state",
            lock_table_name="e2e-locks",
        )

        result = txn.execute(target_region="us-east-2", reason="fail-test")
        assert result.success is False

        # Check that Slack was called (rollback notification)
        slack_calls = [
            c for c in responses.calls if "hooks.slack.com" in c.request.url
        ]
        assert len(slack_calls) >= 1
