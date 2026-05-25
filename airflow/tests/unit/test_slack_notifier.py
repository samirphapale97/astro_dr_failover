"""Unit tests for SlackNotifier."""

from __future__ import annotations

import pytest
import responses

from astro_dr.slack_notifier import SlackNotifier

WEBHOOK_URL = "https://hooks.slack.com/services/T00/B00/xxx"


class TestSlackNotifier:
    @responses.activate
    def test_send_failover_alert_success(self):
        responses.add(responses.POST, WEBHOOK_URL, json={"ok": True}, status=200)

        notifier = SlackNotifier(WEBHOOK_URL, "#test")
        notifier.send_failover_alert(
            from_region="us-east-1",
            to_region="us-east-2",
            reason="health-check failure",
            timestamp="2024-01-01T00:00:00Z",
        )

        assert len(responses.calls) == 1

    @responses.activate
    def test_send_failover_alert_formats_correctly(self):
        responses.add(responses.POST, WEBHOOK_URL, json={"ok": True}, status=200)

        notifier = SlackNotifier(WEBHOOK_URL, "#alerts")
        notifier.send_failover_alert(
            from_region="us-east-1",
            to_region="us-east-2",
            reason="automatic",
            timestamp="2024-06-01T12:00:00Z",
        )

        payload = responses.calls[0].request.body
        assert b"us-east-1" in payload
        assert b"us-east-2" in payload
        assert b"automatic" in payload

    @responses.activate
    def test_send_health_check_alert(self):
        responses.add(responses.POST, WEBHOOK_URL, json={"ok": True}, status=200)

        notifier = SlackNotifier(WEBHOOK_URL, "#test")
        notifier.send_health_check_alert(
            region="us-east-1",
            status="DEGRADED",
            details="SecretsManager latency > 5s",
        )

        assert len(responses.calls) == 1
        payload = responses.calls[0].request.body
        assert b"DEGRADED" in payload

    @responses.activate
    def test_send_recovery_alert(self):
        responses.add(responses.POST, WEBHOOK_URL, json={"ok": True}, status=200)

        notifier = SlackNotifier(WEBHOOK_URL, "#test")
        notifier.send_recovery_alert(region="us-east-1")

        assert len(responses.calls) == 1
        payload = responses.calls[0].request.body
        assert b"Recovered" in payload

    @responses.activate
    def test_webhook_failure_does_not_raise(self):
        """Notification failure must be swallowed, never crash the caller."""
        responses.add(responses.POST, WEBHOOK_URL, json={"error": "invalid_payload"}, status=500)
        # tenacity retries once → 2 total POSTs, both 500, then logs exception
        responses.add(responses.POST, WEBHOOK_URL, json={"error": "invalid_payload"}, status=500)

        notifier = SlackNotifier(WEBHOOK_URL, "#test")
        # Should NOT raise
        notifier.send_failover_alert(
            from_region="us-east-1",
            to_region="us-east-2",
            reason="test",
        )

    @responses.activate
    def test_webhook_timeout_does_not_raise(self):
        """Timeouts must also be swallowed."""
        responses.add(
            responses.POST,
            WEBHOOK_URL,
            body=ConnectionError("timeout"),
        )
        responses.add(
            responses.POST,
            WEBHOOK_URL,
            body=ConnectionError("timeout"),
        )

        notifier = SlackNotifier(WEBHOOK_URL, "#test")
        # Should NOT raise
        notifier.send_failover_alert(
            from_region="us-east-1",
            to_region="us-east-2",
            reason="test",
        )
