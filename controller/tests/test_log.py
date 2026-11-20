import logging

from k8s_inspector.log import setup_logging


def test_http_client_loggers_are_quiet():
    setup_logging("INFO")
    try:
        for name in ("httpx2", "httpcore"):
            assert logging.getLogger(name).getEffectiveLevel() == logging.WARNING
        assert logging.getLogger("k8s_inspector.x").getEffectiveLevel() == logging.INFO
    finally:
        logging.getLogger("httpx2").setLevel(logging.NOTSET)
        logging.getLogger("httpcore").setLevel(logging.NOTSET)
