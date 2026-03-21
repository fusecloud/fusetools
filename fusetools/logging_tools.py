"""Logging utilities."""

from __future__ import annotations

import logging
import sys
from datetime import datetime


# MARK: - log_all
def log_all(filename: str | None = None) -> None:
    """Redirect all stdout/stderr to a timestamped log file.

    :param filename: Base filename to write stdout content to.
    """
    if filename is None:
        return

    dt_str = str(datetime.now()).replace("-", "").replace(":", "").replace(".", "").replace(" ", "_")
    dt_str = dt_str + ".log"

    logger = logging.getLogger()
    sys.stderr.write = logger.error  # type: ignore[assignment]
    sys.stdout.write = logger.info  # type: ignore[assignment]
    logging.basicConfig(filename=filename + "_" + dt_str, level=logging.INFO)
