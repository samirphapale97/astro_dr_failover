"""DynamoDB-backed failover state machine and distributed lock.

Provides ACID-like guarantees for failover operations:

* **Atomicity** — Each failover is tracked as a single DynamoDB item with a
  state machine.  If the process crashes mid-way, the state record survives
  and can be inspected / rolled back.
* **Consistency** — A distributed lock (DynamoDB conditional write) prevents
  two concurrent failovers from colliding.
* **Isolation** — Only one failover can be active at a time (enforced by lock).
* **Durability** — All state transitions are persisted to DynamoDB before the
  next step executes.  Checkpoint data stores previous values so rollback can
  restore exactly what was there before.
"""

from __future__ import annotations

import time
import uuid
from dataclasses import dataclass, field
from datetime import datetime, timezone
from enum import Enum
from typing import Any, Optional

from botocore.exceptions import ClientError

from astro_dr.aws_client import AWSClientFactory
from astro_dr.logger import get_logger

logger = get_logger(__name__)


class FailoverState(str, Enum):
    """States in the failover state machine."""

    INITIATED = "INITIATED"
    LOCK_ACQUIRED = "LOCK_ACQUIRED"
    HEALTH_CHECKED = "HEALTH_CHECKED"
    CONFIG_FETCHED = "CONFIG_FETCHED"
    SQS_NOTIFIED = "SQS_NOTIFIED"
    SSM_UPDATED = "SSM_UPDATED"
    ASTRO_UPDATED = "ASTRO_UPDATED"
    VALIDATED = "VALIDATED"
    COMPLETED = "COMPLETED"
    ROLLING_BACK = "ROLLING_BACK"
    ROLLED_BACK = "ROLLED_BACK"
    FAILED = "FAILED"


@dataclass
class FailoverRecord:
    """Persisted state of a single failover operation."""

    failover_id: str
    state: str
    from_region: str
    to_region: str
    reason: str
    started_at: str
    completed_at: str = ""
    checkpoint_data: dict[str, Any] = field(default_factory=dict)
    error_message: str = ""
    ttl: int = 0  # DynamoDB TTL epoch

    def to_item(self) -> dict[str, Any]:
        """Convert to DynamoDB item format."""
        item: dict[str, Any] = {
            "failover_id": {"S": self.failover_id},
            "state": {"S": self.state},
            "from_region": {"S": self.from_region},
            "to_region": {"S": self.to_region},
            "reason": {"S": self.reason},
            "started_at": {"S": self.started_at},
            "completed_at": {"S": self.completed_at},
            "error_message": {"S": self.error_message},
        }
        if self.checkpoint_data:
            import json

            item["checkpoint_data"] = {"S": json.dumps(self.checkpoint_data)}
        if self.ttl:
            item["ttl"] = {"N": str(self.ttl)}
        return item

    @classmethod
    def from_item(cls, item: dict[str, Any]) -> "FailoverRecord":
        """Parse a DynamoDB item into a FailoverRecord."""
        import json

        checkpoint_raw = item.get("checkpoint_data", {}).get("S", "{}")
        return cls(
            failover_id=item["failover_id"]["S"],
            state=item["state"]["S"],
            from_region=item["from_region"]["S"],
            to_region=item["to_region"]["S"],
            reason=item.get("reason", {}).get("S", ""),
            started_at=item["started_at"]["S"],
            completed_at=item.get("completed_at", {}).get("S", ""),
            checkpoint_data=json.loads(checkpoint_raw),
            error_message=item.get("error_message", {}).get("S", ""),
            ttl=int(item.get("ttl", {}).get("N", "0")),
        )


class FailoverStateManager:
    """DynamoDB-backed state machine for failover operations.

    Parameters
    ----------
    aws_factory:
        Shared :class:`AWSClientFactory`.
    table_name:
        DynamoDB table name for failover state.
    lock_table_name:
        DynamoDB table name for distributed locks.
    region:
        AWS region for DynamoDB.
    lock_ttl_seconds:
        How long a lock is valid before auto-expiry.
    record_ttl_seconds:
        How long completed failover records are kept (default 7 days).
    """

    _LOCK_KEY = "FAILOVER_LOCK"

    def __init__(
        self,
        aws_factory: AWSClientFactory,
        table_name: str = "astro-dr-failover-state",
        lock_table_name: str = "astro-dr-failover-locks",
        region: str = "us-east-1",
        lock_ttl_seconds: int = 300,
        record_ttl_seconds: int = 604800,  # 7 days
    ) -> None:
        self._aws = aws_factory
        self._table_name = table_name
        self._lock_table_name = lock_table_name
        self._region = region
        self._lock_ttl = lock_ttl_seconds
        self._record_ttl = record_ttl_seconds

    @property
    def _client(self):
        return self._aws.get_dynamodb_client(self._region)

    # ── distributed lock ─────────────────────────────────────────────────

    def acquire_lock(self, failover_id: str, owner: str = "") -> bool:
        """Acquire the global failover lock using conditional write.

        Returns ``True`` if the lock was acquired, ``False`` if another
        failover is already holding it.
        """
        now = int(time.time())
        ttl_epoch = now + self._lock_ttl
        owner = owner or f"failover-{failover_id}"

        try:
            self._client.put_item(
                TableName=self._lock_table_name,
                Item={
                    "lock_key": {"S": self._LOCK_KEY},
                    "failover_id": {"S": failover_id},
                    "owner": {"S": owner},
                    "acquired_at": {"N": str(now)},
                    "ttl": {"N": str(ttl_epoch)},
                },
                ConditionExpression=(
                    "attribute_not_exists(lock_key) OR #t < :now"
                ),
                ExpressionAttributeNames={"#t": "ttl"},
                ExpressionAttributeValues={":now": {"N": str(now)}},
            )
            logger.info(
                "Failover lock acquired",
                extra={
                    "operation": "acquire_lock",
                    "ctx": {"failover_id": failover_id, "ttl": ttl_epoch},
                },
            )
            return True
        except ClientError as exc:
            if exc.response["Error"]["Code"] == "ConditionalCheckFailedException":
                logger.warning(
                    "Failover lock already held by another process",
                    extra={"operation": "acquire_lock"},
                )
                return False
            raise

    def release_lock(self, failover_id: str) -> None:
        """Release the failover lock (only if we own it)."""
        try:
            self._client.delete_item(
                TableName=self._lock_table_name,
                Key={"lock_key": {"S": self._LOCK_KEY}},
                ConditionExpression="failover_id = :fid",
                ExpressionAttributeValues={":fid": {"S": failover_id}},
            )
            logger.info(
                "Failover lock released",
                extra={
                    "operation": "release_lock",
                    "ctx": {"failover_id": failover_id},
                },
            )
        except ClientError as exc:
            if exc.response["Error"]["Code"] == "ConditionalCheckFailedException":
                logger.warning(
                    "Lock not owned by this failover, skipping release",
                    extra={"operation": "release_lock"},
                )
            else:
                raise

    def is_locked(self) -> Optional[str]:
        """Check if the failover lock is held.

        Returns the ``failover_id`` of the holder, or ``None``.
        """
        try:
            resp = self._client.get_item(
                TableName=self._lock_table_name,
                Key={"lock_key": {"S": self._LOCK_KEY}},
            )
            item = resp.get("Item")
            if not item:
                return None
            ttl = int(item.get("ttl", {}).get("N", "0"))
            if ttl < int(time.time()):
                return None  # expired
            return item["failover_id"]["S"]
        except ClientError:
            logger.exception("Failed to check lock status")
            return None

    # ── state persistence ────────────────────────────────────────────────

    def create_record(
        self,
        from_region: str,
        to_region: str,
        reason: str,
    ) -> FailoverRecord:
        """Create a new failover record in INITIATED state."""
        now = datetime.now(tz=timezone.utc).isoformat()
        record = FailoverRecord(
            failover_id=str(uuid.uuid4()),
            state=FailoverState.INITIATED,
            from_region=from_region,
            to_region=to_region,
            reason=reason,
            started_at=now,
            ttl=int(time.time()) + self._record_ttl,
        )
        self._client.put_item(
            TableName=self._table_name,
            Item=record.to_item(),
        )
        logger.info(
            "Failover record created",
            extra={
                "operation": "create_record",
                "ctx": {
                    "failover_id": record.failover_id,
                    "from": from_region,
                    "to": to_region,
                },
            },
        )
        return record

    def update_state(
        self,
        failover_id: str,
        new_state: str,
        checkpoint_data: Optional[dict[str, Any]] = None,
        error_message: str = "",
    ) -> None:
        """Transition a failover record to a new state.

        Optionally stores checkpoint data for rollback.
        """
        import json

        update_expr = "SET #s = :state"
        expr_values: dict[str, Any] = {":state": {"S": new_state}}
        expr_names = {"#s": "state"}

        if checkpoint_data:
            update_expr += ", checkpoint_data = :cp"
            expr_values[":cp"] = {"S": json.dumps(checkpoint_data)}

        if error_message:
            update_expr += ", error_message = :err"
            expr_values[":err"] = {"S": error_message}

        if new_state in (
            FailoverState.COMPLETED,
            FailoverState.ROLLED_BACK,
            FailoverState.FAILED,
        ):
            update_expr += ", completed_at = :cat"
            expr_values[":cat"] = {
                "S": datetime.now(tz=timezone.utc).isoformat()
            }

        self._client.update_item(
            TableName=self._table_name,
            Key={"failover_id": {"S": failover_id}},
            UpdateExpression=update_expr,
            ExpressionAttributeNames=expr_names,
            ExpressionAttributeValues=expr_values,
        )
        logger.info(
            "Failover state updated",
            extra={
                "operation": "update_state",
                "ctx": {"failover_id": failover_id, "new_state": new_state},
            },
        )

    def get_record(self, failover_id: str) -> Optional[FailoverRecord]:
        """Retrieve a failover record by ID."""
        try:
            resp = self._client.get_item(
                TableName=self._table_name,
                Key={"failover_id": {"S": failover_id}},
            )
            item = resp.get("Item")
            if not item:
                return None
            return FailoverRecord.from_item(item)
        except ClientError:
            logger.exception("Failed to get failover record")
            return None

    def get_active_failover(self) -> Optional[FailoverRecord]:
        """Find any non-terminal failover record (for crash recovery)."""
        terminal = {
            FailoverState.COMPLETED,
            FailoverState.ROLLED_BACK,
            FailoverState.FAILED,
        }
        try:
            resp = self._client.scan(
                TableName=self._table_name,
                FilterExpression="NOT #s IN (:s1, :s2, :s3)",
                ExpressionAttributeNames={"#s": "state"},
                ExpressionAttributeValues={
                    ":s1": {"S": FailoverState.COMPLETED},
                    ":s2": {"S": FailoverState.ROLLED_BACK},
                    ":s3": {"S": FailoverState.FAILED},
                },
                Limit=1,
            )
            items = resp.get("Items", [])
            if not items:
                return None
            return FailoverRecord.from_item(items[0])
        except ClientError:
            logger.exception("Failed to scan for active failovers")
            return None
