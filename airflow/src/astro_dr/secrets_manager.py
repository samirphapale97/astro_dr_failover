"""Wrapper around AWS Secrets Manager for DR config retrieval.

Provides retry-hardened ``get_secret`` and ``put_secret`` operations with
structured logging and custom error mapping.
"""

from __future__ import annotations

import json
from typing import Any

from botocore.exceptions import ClientError
from tenacity import (
    retry,
    retry_if_exception_type,
    stop_after_attempt,
    wait_exponential_jitter,
)

from astro_dr.aws_client import AWSClientFactory
from astro_dr.config import DRConfig
from astro_dr.exceptions import SecretFetchError
from astro_dr.logger import get_logger

logger = get_logger(__name__)


class SecretsManager:
    """High-level interface to AWS Secrets Manager for DR secrets.

    Parameters
    ----------
    aws_factory:
        Shared :class:`AWSClientFactory` instance.
    config:
        DR configuration.
    """

    def __init__(self, aws_factory: AWSClientFactory, config: DRConfig) -> None:
        self._aws = aws_factory
        self._config = config

    # ── public API ──────────────────────────────────────────────────────

    def get_secret(self, region: str) -> dict[str, Any]:
        """Fetch and parse the JSON secret for *region*.

        Retries transient AWS errors with exponential back-off + jitter.

        Raises
        ------
        SecretFetchError
            On non-retryable errors or exhausted retries.
        """
        secret_name = self._build_secret_name(region)
        logger.info(
            "Fetching secret",
            extra={"region": region, "secret_name": secret_name, "operation": "get_secret"},
        )
        try:
            return self._get_secret_with_retry(region, secret_name)
        except Exception as exc:
            raise SecretFetchError(
                f"Failed to fetch secret after retries: {exc}",
                region=region,
                secret_name=secret_name,
            ) from exc

    def put_secret(self, region: str, values: dict[str, Any]) -> None:
        """Update the secret for *region* with *values*.

        Raises
        ------
        SecretFetchError
            If the update fails.
        """
        secret_name = self._build_secret_name(region)
        logger.info(
            "Updating secret",
            extra={"region": region, "secret_name": secret_name, "operation": "put_secret"},
        )
        client = self._aws.get_secrets_client(region)
        try:
            client.put_secret_value(
                SecretId=secret_name,
                SecretString=json.dumps(values),
            )
            logger.info(
                "Secret updated successfully",
                extra={"region": region, "secret_name": secret_name},
            )
        except ClientError as exc:
            raise SecretFetchError(
                f"Failed to update secret: {exc}",
                region=region,
                secret_name=secret_name,
            ) from exc

    # ── helpers ─────────────────────────────────────────────────────────

    def _build_secret_name(self, region: str) -> str:
        """Format the secret name template with the given *region*."""
        return self._config.secret_name_template.format(region=region)

    @retry(
        retry=retry_if_exception_type(ClientError),
        stop=stop_after_attempt(3),
        wait=wait_exponential_jitter(initial=1, max=10, jitter=2),
        reraise=True,
    )
    def _get_secret_with_retry(self, region: str, secret_name: str) -> dict[str, Any]:
        """Inner retry-wrapped fetch."""
        client = self._aws.get_secrets_client(region)
        try:
            response = client.get_secret_value(SecretId=secret_name)
        except ClientError as exc:
            error_code = exc.response["Error"]["Code"]
            # Do not retry on definitive "not found" — only on transient errors
            if error_code in ("ResourceNotFoundException", "InvalidParameterException"):
                raise SecretFetchError(
                    f"Secret not found: {secret_name}",
                    region=region,
                    secret_name=secret_name,
                ) from exc
            logger.warning(
                "Transient Secrets Manager error, will retry",
                extra={"region": region, "error": str(exc)},
            )
            raise  # allow tenacity to retry

        secret_string: str = response.get("SecretString", "")
        try:
            return json.loads(secret_string)
        except json.JSONDecodeError as exc:
            raise SecretFetchError(
                "Secret value is not valid JSON",
                region=region,
                secret_name=secret_name,
            ) from exc
