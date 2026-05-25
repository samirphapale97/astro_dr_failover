"""Astronomer platform API client.

Provides methods to read and update deployment-level environment variables
via the Astronomer REST API.
"""

from __future__ import annotations

from typing import Any

import requests

from astro_dr.exceptions import AstroAPIError
from astro_dr.logger import get_logger

logger = get_logger(__name__)


class AstroAPIClient:
    """Thin wrapper around the Astronomer REST API.

    Parameters
    ----------
    api_url:
        Base API URL (e.g. ``https://api.astronomer.io``).
    api_key:
        Bearer token for authentication.
    deployment_id:
        ID of the Astro deployment to manage.
    """

    _TIMEOUT_SECONDS = 30

    def __init__(self, api_url: str, api_key: str, deployment_id: str) -> None:
        self._api_url = api_url.rstrip("/")
        self._api_key = api_key
        self._deployment_id = deployment_id

    # ── public API ──────────────────────────────────────────────────────

    def get_deployment_variables(self) -> list[dict[str, Any]]:
        """Retrieve the current environment variables for the deployment.

        Returns
        -------
        list[dict]
            Each dict has ``key``, ``value``, and ``isSecret`` fields.
        """
        path = f"/v1/deployments/{self._deployment_id}/variables"
        data = self._request("GET", path)
        variables: list[dict[str, Any]] = data.get("variables", data if isinstance(data, list) else [])
        logger.info(
            "Retrieved deployment variables",
            extra={
                "operation": "get_deployment_variables",
                "ctx": {"count": len(variables)},
            },
        )
        return variables

    def update_deployment_variables(self, variables: dict[str, str]) -> None:
        """Replace the deployment's environment variables.

        Parameters
        ----------
        variables:
            Mapping of ``VAR_NAME → value`` to set on the deployment.
        """
        path = f"/v1/deployments/{self._deployment_id}/variables"
        payload = [
            {"key": k, "value": v, "isSecret": False}
            for k, v in variables.items()
        ]
        self._request("POST", path, data=payload)
        logger.info(
            "Updated deployment variables",
            extra={
                "operation": "update_deployment_variables",
                "ctx": {"count": len(variables)},
            },
        )

    # ── internal HTTP helper ────────────────────────────────────────────

    def _request(
        self,
        method: str,
        path: str,
        data: Any | None = None,
    ) -> dict[str, Any]:
        """Execute an authenticated HTTP request against the Astro API.

        Raises
        ------
        AstroAPIError
            On any non-2xx response or network error.
        """
        url = f"{self._api_url}{path}"
        headers = {
            "Authorization": f"Bearer {self._api_key}",
            "Content-Type": "application/json",
        }

        logger.info(
            "Astro API request",
            extra={"operation": "astro_api_request", "ctx": {"method": method, "path": path}},
        )

        try:
            response = requests.request(
                method=method,
                url=url,
                headers=headers,
                json=data,
                timeout=self._TIMEOUT_SECONDS,
            )
            response.raise_for_status()
        except requests.exceptions.Timeout as exc:
            raise AstroAPIError(
                f"Request timed out: {method} {path}",
                status_code=0,
            ) from exc
        except requests.exceptions.HTTPError as exc:
            status = exc.response.status_code if exc.response is not None else 0
            body = exc.response.text if exc.response is not None else ""
            raise AstroAPIError(
                f"HTTP {status} from Astro API: {body}",
                status_code=status,
                response_body=body,
            ) from exc
        except requests.exceptions.RequestException as exc:
            raise AstroAPIError(
                f"Network error calling Astro API: {exc}",
                status_code=0,
            ) from exc

        try:
            return response.json()
        except ValueError:
            return {}
