"""Slack notification module for DR failover events.

All public methods swallow exceptions intentionally — a failed notification
must **never** block a failover operation.  Errors are logged instead.
"""

from __future__ import annotations

from datetime import datetime, timezone
from typing import Any

import requests
from tenacity import retry, stop_after_attempt, wait_fixed

from astro_dr.logger import get_logger

logger = get_logger(__name__)


class SlackNotifier:
    """Sends formatted notifications to a Slack incoming webhook.

    Parameters
    ----------
    webhook_url:
        Full Slack webhook URL.
    channel:
        Channel override (e.g. ``#dr-failover-alerts``).
    """

    _TIMEOUT_SECONDS = 10

    def __init__(self, webhook_url: str, channel: str = "#dr-failover-alerts") -> None:
        self._webhook_url = webhook_url
        self._channel = channel

    # ── public alert helpers ────────────────────────────────────────────

    def send_failover_alert(
        self,
        from_region: str,
        to_region: str,
        reason: str,
        timestamp: str | None = None,
    ) -> None:
        """Send a rich failover notification to Slack."""
        ts = timestamp or datetime.now(tz=timezone.utc).isoformat()
        payload = self._build_failover_payload(from_region, to_region, reason, ts)
        try:
            self._send_message(payload)
            logger.info(
                "Failover alert sent to Slack",
                extra={"operation": "send_failover_alert"},
            )
        except Exception:
            logger.exception("Failed to send failover alert to Slack")

    def send_health_check_alert(
        self,
        region: str,
        status: str,
        details: str,
    ) -> None:
        """Send a health-check degradation alert."""
        payload = {
            "channel": self._channel,
            "attachments": [
                {
                    "color": "#FFA500",
                    "blocks": [
                        {
                            "type": "header",
                            "text": {
                                "type": "plain_text",
                                "text": ":warning: Region Health Degraded",
                            },
                        },
                        {
                            "type": "section",
                            "fields": [
                                {"type": "mrkdwn", "text": f"*Region:*\n`{region}`"},
                                {"type": "mrkdwn", "text": f"*Status:*\n`{status}`"},
                                {"type": "mrkdwn", "text": f"*Details:*\n{details}"},
                            ],
                        },
                        {
                            "type": "context",
                            "elements": [
                                {
                                    "type": "mrkdwn",
                                    "text": f":clock1: {datetime.now(tz=timezone.utc).isoformat()}",
                                },
                            ],
                        },
                    ],
                }
            ],
        }
        try:
            self._send_message(payload)
            logger.info(
                "Health check alert sent to Slack",
                extra={"operation": "send_health_check_alert", "region": region},
            )
        except Exception:
            logger.exception("Failed to send health check alert to Slack")

    def send_recovery_alert(self, region: str) -> None:
        """Send a region-recovery notification."""
        payload = {
            "channel": self._channel,
            "attachments": [
                {
                    "color": "#36a64f",
                    "blocks": [
                        {
                            "type": "header",
                            "text": {
                                "type": "plain_text",
                                "text": ":white_check_mark: Region Recovered",
                            },
                        },
                        {
                            "type": "section",
                            "fields": [
                                {"type": "mrkdwn", "text": f"*Region:*\n`{region}`"},
                                {
                                    "type": "mrkdwn",
                                    "text": "*Status:*\n`HEALTHY`",
                                },
                            ],
                        },
                        {
                            "type": "context",
                            "elements": [
                                {
                                    "type": "mrkdwn",
                                    "text": f":clock1: {datetime.now(tz=timezone.utc).isoformat()}",
                                },
                            ],
                        },
                    ],
                }
            ],
        }
        try:
            self._send_message(payload)
            logger.info(
                "Recovery alert sent to Slack",
                extra={"operation": "send_recovery_alert", "region": region},
            )
        except Exception:
            logger.exception("Failed to send recovery alert to Slack")

    # ── internals ───────────────────────────────────────────────────────

    def _build_failover_payload(
        self, from_region: str, to_region: str, reason: str, timestamp: str
    ) -> dict[str, Any]:
        return {
            "channel": self._channel,
            "attachments": [
                {
                    "color": "#FF0000",
                    "blocks": [
                        {
                            "type": "header",
                            "text": {
                                "type": "plain_text",
                                "text": ":rotating_light: DR Failover Executed",
                            },
                        },
                        {
                            "type": "section",
                            "fields": [
                                {
                                    "type": "mrkdwn",
                                    "text": f"*From Region:*\n`{from_region}`",
                                },
                                {
                                    "type": "mrkdwn",
                                    "text": f"*To Region:*\n`{to_region}`",
                                },
                                {
                                    "type": "mrkdwn",
                                    "text": f"*Reason:*\n{reason}",
                                },
                            ],
                        },
                        {
                            "type": "context",
                            "elements": [
                                {
                                    "type": "mrkdwn",
                                    "text": f":clock1: {timestamp}",
                                },
                            ],
                        },
                    ],
                }
            ],
        }

    @retry(stop=stop_after_attempt(2), wait=wait_fixed(1), reraise=True)
    def _send_message(self, payload: dict[str, Any]) -> None:
        """POST *payload* to the Slack webhook with retry on network errors."""
        resp = requests.post(
            self._webhook_url,
            json=payload,
            timeout=self._TIMEOUT_SECONDS,
        )
        resp.raise_for_status()
