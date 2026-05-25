"""Structured JSON logging for the Astro DR Failover system.

Provides a ``get_logger`` helper that configures a standard-library logger
with a JSON formatter.  All log records include *timestamp*, *level*, *name*,
and *message* plus any extra keyword fields the caller supplies.
"""

from __future__ import annotations

import json
import logging
import sys
from datetime import datetime, timezone
from typing import Any


class _JSONFormatter(logging.Formatter):
    """Formats log records as single-line JSON objects."""

    def format(self, record: logging.LogRecord) -> str:
        log_entry: dict[str, Any] = {
            "timestamp": datetime.fromtimestamp(record.created, tz=timezone.utc).isoformat(),
            "level": record.levelname,
            "name": record.name,
            "message": record.getMessage(),
        }
        # Merge any extra context the caller passed via ``logger.info(msg, extra={...})``.
        for key in ("region", "operation", "duration_ms", "secret_name", "error"):
            value = getattr(record, key, None)
            if value is not None:
                log_entry[key] = value

        # Also merge anything stored in a generic ``ctx`` dict.
        ctx: dict[str, Any] | None = getattr(record, "ctx", None)
        if ctx:
            log_entry.update(ctx)

        if record.exc_info and record.exc_info[1] is not None:
            log_entry["exception"] = self.formatException(record.exc_info)

        return json.dumps(log_entry, default=str)


def get_logger(name: str, level: int = logging.INFO) -> logging.Logger:
    """Return a logger configured with JSON-structured output.

    Parameters
    ----------
    name:
        Logger name — typically ``__name__`` of the calling module.
    level:
        Logging level (default ``INFO``).
    """
    logger = logging.getLogger(name)

    if not logger.handlers:
        handler = logging.StreamHandler(sys.stdout)
        handler.setFormatter(_JSONFormatter())
        logger.addHandler(handler)
        logger.setLevel(level)
        logger.propagate = False

    return logger
