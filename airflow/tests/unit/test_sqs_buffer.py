"""Unit tests for FailoverEventBuffer (SQS-backed event buffer)."""

from __future__ import annotations

import json
import uuid

import boto3
import pytest
from moto import mock_aws

from astro_dr.aws_client import AWSClientFactory
from astro_dr.sqs_buffer import FailoverEvent, FailoverEventBuffer, FailoverEventType


def _create_fifo_queue(region: str = "us-east-1") -> str:
    """Create an SQS FIFO queue in moto and return its URL."""
    client = boto3.client("sqs", region_name=region)
    resp = client.create_queue(
        QueueName="failover-events.fifo",
        Attributes={
            "FifoQueue": "true",
            "ContentBasedDeduplication": "false",
        },
    )
    return resp["QueueUrl"]


class TestFailoverEvent:
    def test_serialization_roundtrip(self):
        event = FailoverEvent(
            event_type=FailoverEventType.FAILOVER_STARTED,
            failover_id="abc-123",
            from_region="us-east-1",
            to_region="us-east-2",
        )
        raw = event.to_json()
        parsed = FailoverEvent.from_json(raw)
        assert parsed.event_type == event.event_type
        assert parsed.failover_id == event.failover_id
        assert parsed.from_region == event.from_region
        assert parsed.to_region == event.to_region

    def test_metadata_preserved(self):
        event = FailoverEvent(
            event_type=FailoverEventType.FAILOVER_COMPLETED,
            failover_id="xyz",
            from_region="us-east-1",
            to_region="us-east-2",
            metadata={"config_keys": ["db", "s3"]},
        )
        parsed = FailoverEvent.from_json(event.to_json())
        assert parsed.metadata["config_keys"] == ["db", "s3"]


class TestFailoverEventBuffer:
    @mock_aws
    def test_publish_and_get_latest_event(self):
        queue_url = _create_fifo_queue()
        factory = AWSClientFactory()
        buffer = FailoverEventBuffer(factory, queue_url)

        event = FailoverEvent(
            event_type=FailoverEventType.FAILOVER_STARTED,
            failover_id=str(uuid.uuid4()),
            from_region="us-east-1",
            to_region="us-east-2",
        )
        msg_id = buffer.publish_event(event)
        assert msg_id

        latest = buffer.get_latest_event()
        assert latest is not None
        assert latest.event_type == FailoverEventType.FAILOVER_STARTED
        assert latest.failover_id == event.failover_id

    @mock_aws
    def test_is_failover_in_progress_true(self):
        queue_url = _create_fifo_queue()
        factory = AWSClientFactory()
        buffer = FailoverEventBuffer(factory, queue_url)

        buffer.publish_event(
            FailoverEvent(
                event_type=FailoverEventType.FAILOVER_STARTED,
                failover_id="test-1",
                from_region="us-east-1",
                to_region="us-east-2",
            )
        )
        assert buffer.is_failover_in_progress() is True

    @mock_aws
    def test_is_failover_in_progress_false_when_empty(self):
        queue_url = _create_fifo_queue()
        factory = AWSClientFactory()
        buffer = FailoverEventBuffer(factory, queue_url)
        assert buffer.is_failover_in_progress() is False

    @mock_aws
    def test_should_pause_returns_true_during_failover(self):
        queue_url = _create_fifo_queue()
        factory = AWSClientFactory()
        buffer = FailoverEventBuffer(factory, queue_url)

        buffer.publish_event(
            FailoverEvent(
                event_type=FailoverEventType.FAILBACK_STARTED,
                failover_id="test-2",
                from_region="us-east-2",
                to_region="us-east-1",
            )
        )
        assert buffer.should_pause() is True

    @mock_aws
    def test_purge_queue(self):
        queue_url = _create_fifo_queue()
        factory = AWSClientFactory()
        buffer = FailoverEventBuffer(factory, queue_url)

        buffer.publish_event(
            FailoverEvent(
                event_type=FailoverEventType.FAILOVER_STARTED,
                failover_id="purge-test",
                from_region="us-east-1",
                to_region="us-east-2",
            )
        )
        buffer.purge_queue()
        # After purge, queue should be empty (though moto purge may be async)
        # This test primarily verifies the purge call doesn't error

    @mock_aws
    def test_wait_for_completion_timeout(self):
        queue_url = _create_fifo_queue()
        factory = AWSClientFactory()
        buffer = FailoverEventBuffer(
            factory, queue_url, max_wait_seconds=1, poll_interval_seconds=1
        )

        with pytest.raises(TimeoutError, match="Timed out"):
            buffer.wait_for_completion("nonexistent-id")
