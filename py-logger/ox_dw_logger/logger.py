"""
This is the recommended logger to use to ensure consistency in the way we log.
"""
import logging
import logging.handlers
import os
import time
from .settings import (LOG_DIRECTORY, LOG_DATE_FORMAT, ENV)

LOG_LEVEL = getattr(logging, ENV.get('LOG_LEVEL', 'ERROR'))  # Default ERROR
LOG_FORMAT = '[%(asctime)s] %(levelname)s: %(message)s'
LOG_FORMAT_DEBUG = \
    '[%(asctime)s] %(levelname)s: [%(module)s:%(name)s.%(funcName)s:%(' \
    'lineno)d] %(message)s'
MAX_BYTES = 100000000
BACKUP_COUNT = 5


def get_etl_logger(log_name='etl_logger',
                   log_directory=LOG_DIRECTORY,
                   log_format=None,
                   log_level=LOG_LEVEL,
                   log_date_format=LOG_DATE_FORMAT,
                   max_bytes=MAX_BYTES,
                   backup_count=BACKUP_COUNT):
    """
    Returns a logger at the specified/default location in the specified
    or default format.
    Also note that this will return any named logger that already
    exists despite any options.
    """
    if log_name in logging.Logger.manager.loggerDict.keys():
        return logging.Logger.manager.loggerDict.get(log_name)

    if log_directory is None:
        log_directory = LOG_DIRECTORY

    if isinstance(log_level, str) or log_level is None:
        log_level = getattr(logging, log_level, logging.ERROR)
    # Initialize the logging formatter
    if log_format is None:
        log_format = \
            LOG_FORMAT_DEBUG if log_level == logging.DEBUG else LOG_FORMAT
    formatter = logging.Formatter(log_format, datefmt=log_date_format)
    formatter.converter = time.gmtime

    # Initialize the logger
    logger = logging.getLogger(log_name)
    logger.setLevel(log_level)

    # Add a rotating file based log handler to the logger
    if not os.path.exists(log_directory):
        os.makedirs(log_directory)
    log_file = os.path.join(log_directory, log_name + '.log')
    rotating_file_handler = \
        logging.handlers.RotatingFileHandler(
            log_file, maxBytes=max_bytes, backupCount=backup_count)
    rotating_file_handler.setFormatter(formatter)
    logger.addHandler(rotating_file_handler)

    return logger
