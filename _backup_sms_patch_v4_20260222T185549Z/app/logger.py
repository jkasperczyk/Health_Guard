from __future__ import annotations
import logging

def setup_logger(_log_path: str) -> logging.Logger:
    # In systemd we append stdout/stderr to a log file, so here we keep a simple console logger.
    logger = logging.getLogger("weatherguard")
    logger.setLevel(logging.INFO)

    if not logger.handlers:
        handler = logging.StreamHandler()
        fmt = logging.Formatter("%(asctime)s | %(levelname)s | %(message)s")
        handler.setFormatter(fmt)
        logger.addHandler(handler)

    return logger
