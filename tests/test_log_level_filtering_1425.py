"""REVOPS-1425: the arq worker must actually filter log levels.

structlog's default wrapper does no level filtering, so per-step chatter
(capacity defers, send-window defers) demoted to debug would STILL reach the
launchd log file. Importing src.workers.main configures a filtering wrapper at
settings.log_level (INFO default); these tests pin that contract.
"""
import logging
import os
import sys

import structlog

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

import src.workers.main  # noqa: F401  (import runs structlog.configure)


def test_debug_is_filtered_info_passes(capsys):
    logger = structlog.get_logger()
    logger.debug("chatty per-step line", mailbox_id="m1")
    logger.info("aggregate summary line")
    out = capsys.readouterr().out
    assert "chatty per-step line" not in out, (
        "debug must be filtered — otherwise the demoted capacity/send-window "
        "chatter still fills scout-arq-worker.log"
    )
    assert "aggregate summary line" in out


def test_wrapper_filters_at_configured_level():
    wrapper = structlog.get_config()["wrapper_class"]
    # make_filtering_bound_logger stubs out methods below the cutoff.
    from src.config import get_settings

    level = getattr(logging, get_settings().log_level.upper(), logging.INFO)
    assert wrapper is structlog.make_filtering_bound_logger(level)
