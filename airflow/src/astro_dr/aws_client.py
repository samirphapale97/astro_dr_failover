"""Thread-safe AWS client factory.

Creates and caches boto3 service clients keyed by ``(service, region)`` so
that multiple modules can share the same underlying HTTP connection pools
without risking race conditions on client creation.
"""

from __future__ import annotations

import threading
from typing import Any

import boto3
from botocore.client import BaseClient

from astro_dr.logger import get_logger

logger = get_logger(__name__)


class AWSClientFactory:
    """Creates and caches boto3 clients in a thread-safe manner.

    Parameters
    ----------
    session:
        Optional pre-configured ``boto3.Session``.  If *None* the default
        session is created lazily.
    """

    def __init__(self, session: boto3.Session | None = None) -> None:
        self._session = session or boto3.Session()
        self._clients: dict[tuple[str, str], BaseClient] = {}
        self._lock = threading.Lock()

    # ── public helpers ──────────────────────────────────────────────────

    def get_secrets_client(self, region: str) -> BaseClient:
        """Return a Secrets Manager client for *region*."""
        return self._get_client("secretsmanager", region)

    def get_ssm_client(self, region: str) -> BaseClient:
        """Return an SSM client for *region*."""
        return self._get_client("ssm", region)

    def get_sts_client(self) -> BaseClient:
        """Return an STS client (region-agnostic, defaults to us-east-1)."""
        return self._get_client("sts", "us-east-1")

    def get_sqs_client(self, region: str) -> BaseClient:
        """Return an SQS client for *region*."""
        return self._get_client("sqs", region)

    def get_dynamodb_client(self, region: str) -> BaseClient:
        """Return a DynamoDB client for *region*."""
        return self._get_client("dynamodb", region)

    # ── internal ────────────────────────────────────────────────────────

    def _get_client(self, service: str, region: str) -> Any:
        key = (service, region)
        if key not in self._clients:
            with self._lock:
                # Double-checked locking
                if key not in self._clients:
                    logger.info(
                        "Creating boto3 client",
                        extra={"ctx": {"service": service, "region": region}},
                    )
                    self._clients[key] = self._session.client(
                        service, region_name=region
                    )
        return self._clients[key]
