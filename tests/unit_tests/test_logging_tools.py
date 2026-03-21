"""Tests for logging_tools module."""

import logging
import sys
import tempfile

from fusetools.logging_tools import log_all


def test_log_all_redirects_streams() -> None:
    """log_all with a filename should redirect sys.stdout/stderr to logger methods."""
    orig_stdout_write = sys.stdout.write
    orig_stderr_write = sys.stderr.write
    try:
        with tempfile.TemporaryDirectory() as tmpdir:
            base = tmpdir + "/test_log"
            log_all(filename=base)

            logger = logging.getLogger()
            assert sys.stdout.write == logger.info  # type: ignore[comparison-overlap]
            assert sys.stderr.write == logger.error  # type: ignore[comparison-overlap]
    finally:
        sys.stdout.write = orig_stdout_write  # type: ignore[method-assign]
        sys.stderr.write = orig_stderr_write  # type: ignore[method-assign]


def test_log_all_none_is_noop() -> None:
    """log_all with no filename should be a no-op."""
    log_all(filename=None)
