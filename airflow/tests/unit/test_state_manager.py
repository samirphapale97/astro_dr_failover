"""Unit tests for FailoverStateManager (DynamoDB-backed state machine + lock)."""

from __future__ import annotations

import json
import time
import uuid

import boto3
import pytest
from moto import mock_aws

from astro_dr.aws_client import AWSClientFactory
from astro_dr.state_manager import (
    FailoverRecord,
    FailoverState,
    FailoverStateManager,
)


def _create_tables(region: str = "us-east-1") -> tuple[str, str]:
    """Create DynamoDB tables in moto and return (state_table, lock_table)."""
    client = boto3.client("dynamodb", region_name=region)

    state_table = "test-failover-state"
    client.create_table(
        TableName=state_table,
        KeySchema=[{"AttributeName": "failover_id", "KeyType": "HASH"}],
        AttributeDefinitions=[
            {"AttributeName": "failover_id", "AttributeType": "S"}
        ],
        BillingMode="PAY_PER_REQUEST",
    )

    lock_table = "test-failover-locks"
    client.create_table(
        TableName=lock_table,
        KeySchema=[{"AttributeName": "lock_key", "KeyType": "HASH"}],
        AttributeDefinitions=[
            {"AttributeName": "lock_key", "AttributeType": "S"}
        ],
        BillingMode="PAY_PER_REQUEST",
    )

    return state_table, lock_table


class TestDistributedLock:
    @mock_aws
    def test_acquire_lock_success(self):
        state_table, lock_table = _create_tables()
        factory = AWSClientFactory()
        mgr = FailoverStateManager(
            factory, table_name=state_table, lock_table_name=lock_table
        )

        fid = str(uuid.uuid4())
        assert mgr.acquire_lock(fid) is True

    @mock_aws
    def test_acquire_lock_fails_when_held(self):
        state_table, lock_table = _create_tables()
        factory = AWSClientFactory()
        mgr = FailoverStateManager(
            factory, table_name=state_table, lock_table_name=lock_table
        )

        fid1 = str(uuid.uuid4())
        fid2 = str(uuid.uuid4())
        assert mgr.acquire_lock(fid1) is True
        assert mgr.acquire_lock(fid2) is False

    @mock_aws
    def test_release_lock_allows_reacquire(self):
        state_table, lock_table = _create_tables()
        factory = AWSClientFactory()
        mgr = FailoverStateManager(
            factory, table_name=state_table, lock_table_name=lock_table
        )

        fid1 = str(uuid.uuid4())
        fid2 = str(uuid.uuid4())
        mgr.acquire_lock(fid1)
        mgr.release_lock(fid1)
        assert mgr.acquire_lock(fid2) is True

    @mock_aws
    def test_release_lock_wrong_owner_ignored(self):
        state_table, lock_table = _create_tables()
        factory = AWSClientFactory()
        mgr = FailoverStateManager(
            factory, table_name=state_table, lock_table_name=lock_table
        )

        fid1 = str(uuid.uuid4())
        fid2 = str(uuid.uuid4())
        mgr.acquire_lock(fid1)
        mgr.release_lock(fid2)  # wrong owner — should not release
        assert mgr.acquire_lock(fid2) is False  # still locked

    @mock_aws
    def test_is_locked_returns_holder(self):
        state_table, lock_table = _create_tables()
        factory = AWSClientFactory()
        mgr = FailoverStateManager(
            factory, table_name=state_table, lock_table_name=lock_table
        )

        assert mgr.is_locked() is None
        fid = str(uuid.uuid4())
        mgr.acquire_lock(fid)
        assert mgr.is_locked() == fid

    @mock_aws
    def test_expired_lock_can_be_reacquired(self):
        state_table, lock_table = _create_tables()
        factory = AWSClientFactory()
        mgr = FailoverStateManager(
            factory,
            table_name=state_table,
            lock_table_name=lock_table,
            lock_ttl_seconds=0,  # expires immediately
        )

        fid1 = str(uuid.uuid4())
        mgr.acquire_lock(fid1)
        time.sleep(0.1)
        fid2 = str(uuid.uuid4())
        assert mgr.acquire_lock(fid2) is True


class TestFailoverStateManagement:
    @mock_aws
    def test_create_record(self):
        state_table, lock_table = _create_tables()
        factory = AWSClientFactory()
        mgr = FailoverStateManager(
            factory, table_name=state_table, lock_table_name=lock_table
        )

        record = mgr.create_record("us-east-1", "us-east-2", "test")
        assert record.failover_id
        assert record.state == FailoverState.INITIATED
        assert record.from_region == "us-east-1"
        assert record.to_region == "us-east-2"

    @mock_aws
    def test_update_state_and_get_record(self):
        state_table, lock_table = _create_tables()
        factory = AWSClientFactory()
        mgr = FailoverStateManager(
            factory, table_name=state_table, lock_table_name=lock_table
        )

        record = mgr.create_record("us-east-1", "us-east-2", "test")
        mgr.update_state(
            record.failover_id,
            FailoverState.SSM_UPDATED,
            checkpoint_data={"ssm_previous_value": "us-east-1"},
        )

        fetched = mgr.get_record(record.failover_id)
        assert fetched is not None
        assert fetched.state == FailoverState.SSM_UPDATED
        assert fetched.checkpoint_data["ssm_previous_value"] == "us-east-1"

    @mock_aws
    def test_completed_record_has_completed_at(self):
        state_table, lock_table = _create_tables()
        factory = AWSClientFactory()
        mgr = FailoverStateManager(
            factory, table_name=state_table, lock_table_name=lock_table
        )

        record = mgr.create_record("us-east-1", "us-east-2", "test")
        mgr.update_state(record.failover_id, FailoverState.COMPLETED)

        fetched = mgr.get_record(record.failover_id)
        assert fetched is not None
        assert fetched.completed_at != ""

    @mock_aws
    def test_get_active_failover_finds_nonterminal(self):
        state_table, lock_table = _create_tables()
        factory = AWSClientFactory()
        mgr = FailoverStateManager(
            factory, table_name=state_table, lock_table_name=lock_table
        )

        record = mgr.create_record("us-east-1", "us-east-2", "test")
        mgr.update_state(record.failover_id, FailoverState.SSM_UPDATED)

        active = mgr.get_active_failover()
        assert active is not None
        assert active.failover_id == record.failover_id

    @mock_aws
    def test_get_active_failover_ignores_completed(self):
        state_table, lock_table = _create_tables()
        factory = AWSClientFactory()
        mgr = FailoverStateManager(
            factory, table_name=state_table, lock_table_name=lock_table
        )

        record = mgr.create_record("us-east-1", "us-east-2", "test")
        mgr.update_state(record.failover_id, FailoverState.COMPLETED)

        active = mgr.get_active_failover()
        assert active is None

    @mock_aws
    def test_update_with_error_message(self):
        state_table, lock_table = _create_tables()
        factory = AWSClientFactory()
        mgr = FailoverStateManager(
            factory, table_name=state_table, lock_table_name=lock_table
        )

        record = mgr.create_record("us-east-1", "us-east-2", "test")
        mgr.update_state(
            record.failover_id,
            FailoverState.FAILED,
            error_message="SSM update timed out",
        )

        fetched = mgr.get_record(record.failover_id)
        assert fetched.error_message == "SSM update timed out"


class TestFailoverRecord:
    def test_to_item_roundtrip(self):
        record = FailoverRecord(
            failover_id="test-123",
            state=FailoverState.SSM_UPDATED,
            from_region="us-east-1",
            to_region="us-east-2",
            reason="integration-test",
            started_at="2024-01-01T00:00:00+00:00",
            checkpoint_data={"ssm_previous_value": "us-east-1"},
        )
        item = record.to_item()
        parsed = FailoverRecord.from_item(item)
        assert parsed.failover_id == record.failover_id
        assert parsed.state == record.state
        assert parsed.checkpoint_data == record.checkpoint_data
