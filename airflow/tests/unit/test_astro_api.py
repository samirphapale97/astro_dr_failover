"""Unit tests for AstroAPIClient."""

from __future__ import annotations

import pytest
import responses

from astro_dr.astro_api import AstroAPIClient
from astro_dr.exceptions import AstroAPIError

API_URL = "https://api.astronomer.io"
API_KEY = "test-bearer-token"
DEPLOYMENT_ID = "dep-abc123"
VARS_PATH = f"/v1/deployments/{DEPLOYMENT_ID}/variables"


@pytest.fixture()
def client() -> AstroAPIClient:
    return AstroAPIClient(API_URL, API_KEY, DEPLOYMENT_ID)


class TestAstroAPIClient:
    @responses.activate
    def test_get_deployment_variables(self, client):
        responses.add(
            responses.GET,
            f"{API_URL}{VARS_PATH}",
            json={
                "variables": [
                    {"key": "DATABRICKS_URL", "value": "https://db.com", "isSecret": False},
                ]
            },
            status=200,
        )

        result = client.get_deployment_variables()
        assert len(result) == 1
        assert result[0]["key"] == "DATABRICKS_URL"

    @responses.activate
    def test_update_deployment_variables(self, client):
        responses.add(
            responses.POST,
            f"{API_URL}{VARS_PATH}",
            json={"variables": []},
            status=200,
        )

        client.update_deployment_variables({"DATABRICKS_URL": "https://new.db.com"})
        assert len(responses.calls) == 1

        # Check the request payload
        import json

        payload = json.loads(responses.calls[0].request.body)
        assert payload == [{"key": "DATABRICKS_URL", "value": "https://new.db.com", "isSecret": False}]

    @responses.activate
    def test_auth_header_included(self, client):
        responses.add(
            responses.GET,
            f"{API_URL}{VARS_PATH}",
            json={"variables": []},
            status=200,
        )

        client.get_deployment_variables()
        auth = responses.calls[0].request.headers["Authorization"]
        assert auth == f"Bearer {API_KEY}"

    @responses.activate
    def test_api_error_raises_astro_api_error(self, client):
        responses.add(
            responses.GET,
            f"{API_URL}{VARS_PATH}",
            json={"error": "forbidden"},
            status=403,
        )

        with pytest.raises(AstroAPIError) as exc_info:
            client.get_deployment_variables()
        assert exc_info.value.status_code == 403

    @responses.activate
    def test_timeout_handling(self, client):
        responses.add(
            responses.GET,
            f"{API_URL}{VARS_PATH}",
            body=ConnectionError("timed out"),
        )

        with pytest.raises(AstroAPIError, match="Network error"):
            client.get_deployment_variables()

    @responses.activate
    def test_empty_response_body(self, client):
        """Non-JSON 200 response should return empty dict."""
        responses.add(
            responses.POST,
            f"{API_URL}{VARS_PATH}",
            body="",
            status=200,
            content_type="text/plain",
        )

        # Should not raise
        client.update_deployment_variables({"KEY": "val"})
