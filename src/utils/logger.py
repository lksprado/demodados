import logging
import os
import sys
from logging.handlers import RotatingFileHandler


def logger_setting(name: str = "demodados", level=logging.INFO):
    logger = logging.getLogger(name)

    if not logger.hasHandlers():
        logger.setLevel(level)

        formatter = logging.Formatter(
            fmt="%(asctime)s | %(name)s | %(levelname)s | %(message)s",
            datefmt="%Y-%m-%d %H:%M:%S",
        )

        # Console handler
        stream_handler = logging.StreamHandler(sys.stdout)
        stream_handler.setFormatter(formatter)
        logger.addHandler(stream_handler)

        # Diretório de logs
        os.makedirs("logs", exist_ok=True)

        # Apenas 1 arquivo por pipeline (sem backup)
        file_handler = RotatingFileHandler(
            filename=f"src/pipelines/logs/{name}.log",
            maxBytes=10 * 1024 * 1024,  # 10MB
            backupCount=0,
        )
        file_handler.setFormatter(formatter)
        logger.addHandler(file_handler)

    return logger
