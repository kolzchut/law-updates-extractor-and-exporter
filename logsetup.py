"""Logging setup for the law-updates scraper.

Two output formats:

- ``text`` (default) — the human-readable ``LEVEL:logger:message`` form, for
  interactive/local runs.
- ``json`` — one JSON object per line, so Azure Log Analytics can pivot on
  structured fields (level, logger, and any ``extra=`` fields such as the
  end-of-run ``run_summary``). The scheduled Container Apps Job runs with
  ``--log-format json``.
"""

import json
import logging


# Standard LogRecord attributes — everything else on a record came from an
# ``extra=`` dict and should be merged into the JSON output.
_RESERVED = set(vars(logging.makeLogRecord({}))) | {"message", "asctime"}


class JsonFormatter(logging.Formatter):
    """Emit each log record as a single-line JSON object.

    ``ensure_ascii=False`` keeps Hebrew booklet names readable rather than
    escaping them to ``\\uXXXX``.
    """

    def format(self, record):
        payload = {
            "time": self.formatTime(record),
            "level": record.levelname,
            "logger": record.name,
            "message": record.getMessage(),
        }
        for key, value in record.__dict__.items():
            if key not in _RESERVED and not key.startswith("_"):
                payload[key] = value
        if record.exc_info:
            payload["exc_info"] = self.formatException(record.exc_info)
        return json.dumps(payload, ensure_ascii=False, default=str)


def configure_logging(level, log_format):
    """Install a single root handler with the chosen formatter.

    Clears any pre-existing handlers so repeated calls (and the implicit
    handler ``logging`` lazily installs) don't double-log.
    """
    handler = logging.StreamHandler()
    if log_format == "json":
        handler.setFormatter(JsonFormatter())
    else:
        handler.setFormatter(logging.Formatter("%(levelname)s:%(name)s:%(message)s"))

    root = logging.getLogger()
    root.handlers.clear()
    root.setLevel(level)
    root.addHandler(handler)
