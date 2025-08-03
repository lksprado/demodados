import logging
import sys

def setup_logging(name: str = "demodados", level=logging.INFO):
    logger = logging.getLogger(name)
    if not logger.hasHandlers():
        logger.setLevel(level)

        formatter = logging.Formatter(
            fmt='%(asctime)s | %(name)s | %(levelname)s | %(message)s',
            datefmt='%Y-%m-%d %H:%M:%S'
        )

        handler = logging.StreamHandler(sys.stdout)
        handler.setFormatter(formatter)

        logger.addHandler(handler)

    return logger
