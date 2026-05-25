"""SQS-based event buffer for zero-loss failover transitions.

During a failover, there is a window where:
- The SSM parameter is being updated
- Secrets in the target region may not yet be verified
- Astro env vars are being pushed

Any DAG that reads config during this window could get **stale** or
**inconsistent** data.  The ``FailoverEventBuffer`` solves this by:

1. Publishing a ``FAILOVER_STARTED`` event to SQS before any mutation.
2. DAGs call ``should_pause()`` — if a FAILOVER_STARTED event is in-flight,
   the DAG pauses (sleeps + retries) until a ``FAILOVER_COMPLETED`` or
   ``FAILOVER_ROLLED_BACK`` event arrives.
3. After the failover completes (or rolls back), a completion event is
   published and the pause is released.

This guarantees that **no DAG reads config during the transition window**.
"""

from __future__ import annotations

import json
import time
from dataclasses import asdict, dataclass, field
from datetime import datetime, timezone
from enum import Enum
from typing import Any, Optional

from botocore.exceptions import ClientError

from astro_dr.aws_client import AWSClientFactory
from astro_dr.logger import get_logger

logger = get_logger(__name__)


class FailoverEventType(str, Enum):
    """Types of events published to the SQS buffer."""

    FAILOVER_STARTED = "FAILOVER_STARTED"
    FAILOVER_COMPLETED = "FAILOVER_COMPLETED"
    FAILOVER_ROLLED_BACK = "FAILOVER_ROLLED_BACK"
    FAILBACK_STARTED = "FAILBACK_STARTED"
    FAILBACK_COMPLETED = "FAILBACK_COMPLETED"


@dataclass
class FailoverEvent:
    """Represents a single failover lifecycle event."""

    event_type: str
    failover_id: str
    from_region: str
    to_region: str
    timestamp: str = field(
        default_factory=lambda: datetime.now(tz=timezone.utc).isoformat()
    )
    metadata: dict[str, Any] = field(default_factory=dict)

    def to_json(self) -> str:
        return json.dumps(asdict(self))

    @classmethod
    def from_json(cls, raw: str) -> "FailoverEvent":
        data = json.loads(raw)
        return cls(**data)


class FailoverEventBuffer:
    """SQS-backed event buffer for gating DAG reads during failover.

    Parameters
    ----------
    aws_factory:
        Shared :class:`AWSClientFactory`.
    queue_url:
        Full SQS queue URL.
    region:
        AWS region where the SQS queue lives.
    poll_interval_seconds:
        How often ``wait_for_completion`` polls the queue.
    max_wait_seconds:
        Maximum time ``wait_for_completion`` will block before raising.
    """

    def __init__(
        self,
        aws_factory: AWSClientFactory,
        queue_url: str,
        region: str = "us-east-1",
        poll_interval_seconds: int = 2,
        max_wait_seconds: int = 120,
    ) -> None:
        self._aws = aws_factory
        self._queue_url = queue_url
        self._region = region
        self._poll_interval = poll_interval_seconds
        self._max_wait = max_wait_seconds

    @property
    def _client(self):
        return self._aws.get_sqs_client(self._region)

    # ── publish ──────────────────────────────────────────────────────────

    def publish_event(self, event: FailoverEvent) -> str:
        """Publish a failover event to the SQS queue.

        Returns the SQS MessageId.
        """
        try:
            resp = self._client.send_message(
                QueueUrl=self._queue_url,
                MessageBody=event.to_json(),
                MessageGroupId="failover-events",
                MessageDeduplicationId=f"{event.failover_id}-{event.event_type}",
                MessageAttributes={
                    "event_type": {
                        "DataType": "String",
                        "StringValue": event.event_type,
                    },
                    "failover_id": {
                        "DataType": "String",
                        "StringValue": event.failover_id,
                    },
                },
            )
            msg_id = resp["MessageId"]
            logger.info(
                "Published failover event to SQS",
                extra={
                    "operation": "publish_event",
                    "ctx": {
                        "event_type": event.event_type,
                        "failover_id": event.failover_id,
                        "message_id": msg_id,
                    },
                },
            )
            return msg_id
        except ClientError as exc:
            logger.exception("Failed to publish event to SQS")
            raise

    # ── consume / gate ───────────────────────────────────────────────────

    def get_latest_event(self) -> Optional[FailoverEvent]:
        """Peek at the most recent event without removing it.

        Returns ``None`` if the queue is empty.
        """
        try:
            resp = self._client.receive_message(
                QueueUrl=self._queue_url,
                MaxNumberOfMessages=1,
                WaitTimeSeconds=0,
                VisibilityTimeout=0,  # don't hide it from other consumers
            )
            messages = resp.get("Messages", [])
            if not messages:
                return None
            return FailoverEvent.from_json(messages[0]["Body"])
        except ClientError:
            logger.exception("Failed to read SQS event")
            return None

    def is_failover_in_progress(self) -> bool:
        """Check if a failover is currently in progress.

        Returns ``True`` if the latest event is a STARTED event without
        a matching COMPLETED/ROLLED_BACK.
        """
        event = self.get_latest_event()
        if event is None:
            return False
        return event.event_type in (
            FailoverEventType.FAILOVER_STARTED,
            FailoverEventType.FAILBACK_STARTED,
        )

    def should_pause(self) -> bool:
        """DAGs should call this before reading config.

        Returns ``True`` if the DAG should wait before reading config.
        """
        return self.is_failover_in_progress()

    def wait_for_completion(self, failover_id: str) -> FailoverEvent:
        """Block until a completion event for *failover_id* appears.

        Raises ``TimeoutError`` if ``max_wait_seconds`` is exceeded.
        """
        deadline = time.monotonic() + self._max_wait
        while time.monotonic() < deadline:
            try:
                resp = self._client.receive_message(
                    QueueUrl=self._queue_url,
                    MaxNumberOfMessages=10,
                    WaitTimeSeconds=min(self._poll_interval, 5),
                    MessageAttributeNames=["All"],
                )
                for msg in resp.get("Messages", []):
                    event = FailoverEvent.from_json(msg["Body"])
                    if (
                        event.failover_id == failover_id
                        and event.event_type
                        in (
                            FailoverEventType.FAILOVER_COMPLETED,
                            FailoverEventType.FAILOVER_ROLLED_BACK,
                            FailoverEventType.FAILBACK_COMPLETED,
                        )
                    ):
                        # Delete the message now that it's been consumed
                        self._client.delete_message(
                            QueueUrl=self._queue_url,
                            ReceiptHandle=msg["ReceiptHandle"],
                        )
                        logger.info(
                            "Failover completion event received",
                            extra={
                                "operation": "wait_for_completion",
                                "ctx": {
                                    "failover_id": failover_id,
                                    "event_type": event.event_type,
                                },
                            },
                        )
                        return event
            except ClientError:
                logger.exception("Error polling SQS for completion")

            time.sleep(self._poll_interval)

        raise TimeoutError(
            f"Timed out waiting for failover {failover_id} to complete "
            f"after {self._max_wait}s"
        )

    # ── cleanup ──────────────────────────────────────────────────────────

    def purge_queue(self) -> None:
        """Remove all messages from the queue (use with caution)."""
        try:
            self._client.purge_queue(QueueUrl=self._queue_url)
            logger.info("SQS queue purged", extra={"operation": "purge_queue"})
        except ClientError:
            logger.exception("Failed to purge SQS queue")
