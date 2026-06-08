import logging
import sys
from logging.handlers import RotatingFileHandler
from pathlib import Path
from typing import Optional


def logger_setting(
    name: str = "demodados", level=logging.INFO, log_file: Optional[str] = None
):
    logger = logging.getLogger(name)

    if not logger.hasHandlers():
        logger.setLevel(level)

        formatter = logging.Formatter(
            fmt="%(asctime)s | %(name)s | %(levelname)s | %(message)s",
            datefmt="%Y-%m-%d %H:%M:%S",
        )

        stream_handler = logging.StreamHandler(sys.stdout)
        stream_handler.setFormatter(formatter)
        logger.addHandler(stream_handler)

        if log_file:
            Path(log_file).parent.mkdir(parents=True, exist_ok=True)
            file_handler = RotatingFileHandler(
                filename=log_file,
                maxBytes=10 * 1024 * 1024,  # 10MB
                backupCount=0,
            )
            file_handler.setFormatter(formatter)
            logger.addHandler(file_handler)

    return logger
