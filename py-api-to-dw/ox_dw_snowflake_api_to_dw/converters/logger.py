"""
This will get the appropriate logger.
"""
import logging
from ox_dw_logger import get_etl_logger
from ..settings import APP_NAME, HARMONIZER_NAME

DEFAULT = 'default'


def get_logger():
    """
    Will look at the known loggers for this package.
    Will return
    """
    for name in [APP_NAME, HARMONIZER_NAME, DEFAULT]:
        logger = logging.Logger.manager.loggerDict.get(name)
        if logger is not None:
            return logger

    return get_etl_logger(DEFAULT, log_directory=None)
