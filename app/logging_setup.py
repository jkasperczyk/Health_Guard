import os
import logging
from logging.handlers import RotatingFileHandler

def setup_logging(log_dir: str) -> None:
    os.makedirs(log_dir, exist_ok=True)

    logger = logging.getLogger()
    logger.setLevel(logging.INFO)

    # Usuń istniejące handlery, żeby nie duplikować logów przy kolejnych uruchomieniach
    for h in list(logger.handlers):
        logger.removeHandler(h)

    fmt = logging.Formatter("%(asctime)s | %(levelname)s | %(message)s")

    file_path = os.path.join(log_dir, "weatherguard.log")
    fh = RotatingFileHandler(file_path, maxBytes=2_000_000, backupCount=5)
    fh.setFormatter(fmt)
    fh.setLevel(logging.INFO)

    sh = logging.StreamHandler()
    sh.setFormatter(fmt)
    sh.setLevel(logging.INFO)

    logger.addHandler(fh)
    logger.addHandler(sh)
