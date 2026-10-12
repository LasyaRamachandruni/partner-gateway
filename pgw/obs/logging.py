"""Structured logging: one JSON object per line on stderr, with the trace id when there is one.

Cloud Logging parses `severity` and `message` from JSON lines and links entries to
Cloud Trace through `logging.googleapis.com/trace`. Fields passed with
`extra={...}` are included as top-level keys.
"""

from __future__ import annotations

import json
import logging
import os
import sys
from datetime import UTC, datetime
from typing import Any

from . import tracing

_STANDARD = set(vars(logging.LogRecord("", 0, "", 0, "", (), None))) | {"message", "asctime", "taskName"}


class JsonFormatter(logging.Formatter):
    def __init__(self, service: str) -> None:
        super().__init__()
        self.service = service
        self.project = os.environ.get("GOOGLE_CLOUD_PROJECT")

    def format(self, record: logging.LogRecord) -> str:
        out: dict[str, Any] = {
            "time": datetime.fromtimestamp(record.created, UTC).isoformat(),
            "severity": record.levelname,
            "logger": record.name,
            "service": self.service,
            "message": record.getMessage(),
        }
        trace_id = tracing.current_trace_id()
        if trace_id:
            out["trace_id"] = trace_id
            if self.project:
                out["logging.googleapis.com/trace"] = f"projects/{self.project}/traces/{trace_id}"
        for k, v in vars(record).items():
            if k not in _STANDARD and not k.startswith("_"):
                out[k] = v
        if record.exc_info:
            out["exception"] = self.formatException(record.exc_info)
        return json.dumps(out, default=str)


def setup(service: str, level: str = "INFO", fmt: str = "json") -> None:
    handler = logging.StreamHandler(sys.stderr)  # stdout is for command output (credentials, key ids)
    if fmt == "json":
        handler.setFormatter(JsonFormatter(service))
    else:
        handler.setFormatter(logging.Formatter("%(asctime)s %(levelname)s %(name)s: %(message)s"))
    root = logging.getLogger()
    root.handlers[:] = [handler]
    root.setLevel(level.upper())
    for noisy in ("httpx", "httpcore", "grpc"):
        logging.getLogger(noisy).setLevel(max(logging.WARNING, root.level))
