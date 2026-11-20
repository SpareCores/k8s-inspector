"""Logging setup: stdlib logging, one JSON object per line on stderr.

Use `logging.getLogger(__name__)` everywhere and pass context as `extra={"run": name, "task": key}`; the
formatter emits every `extra` field. Never log presigned URLs, tokens or key file contents.
"""

import json
import logging
import sys
from datetime import UTC, datetime

# Attributes every LogRecord has; anything else on a record came in through `extra=`.
_STANDARD = set(vars(logging.LogRecord("", 0, "", 0, "", None, None))) | {"message", "asctime", "taskName"}


class JsonFormatter(logging.Formatter):
    def format(self, record: logging.LogRecord) -> str:
        doc = {
            "ts": datetime.fromtimestamp(record.created, UTC).isoformat(timespec="milliseconds"),
            "level": record.levelname.lower(),
            "logger": record.name,
            "msg": record.getMessage(),
        }
        doc.update({k: v for k, v in vars(record).items() if k not in _STANDARD})
        if record.exc_info:
            doc["exc"] = self.formatException(record.exc_info)
        return json.dumps(doc, default=str)


def setup_logging(level: str = "INFO") -> None:
    handler = logging.StreamHandler(sys.stderr)
    handler.setFormatter(JsonFormatter())
    logging.basicConfig(level=level, handlers=[handler], force=True)
    # lightkube's HTTP client logs every request at info; that is noise at controller scale.
    for noisy in ("httpx2", "httpcore"):
        logging.getLogger(noisy).setLevel(logging.WARNING)
