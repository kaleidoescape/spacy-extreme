import logging
import sys
import os

def setup_logger(folder, name='parallel', level=logging.INFO):
    """Write logs to the filepath at folder/name.log and also to stdout."""
    os.makedirs(folder, exist_ok=True)
    fp = os.path.join(folder, name + '.log')

    file_handler = logging.FileHandler(fp)
    file_formatter = logging.Formatter(
        '[%(asctime)s:%(levelname)s:%(name)s:%(funcName)s] %(message)s')
    file_handler.setFormatter(file_formatter)

    stream_handler = logging.StreamHandler(sys.stdout)
    stream_formatter = logging.Formatter(
        '[%(levelname)s:%(name)s] %(message)s')
    stream_handler.setFormatter(stream_formatter)

    logger = logging.getLogger(name)
    logger.setLevel(level)

    if not logger.handlers:
        logger.addHandler(file_handler)
        logger.addHandler(stream_handler)

    return logger