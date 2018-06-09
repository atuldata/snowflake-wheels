"""
Common function for download and upload.
"""
import logging
import sys
from pid import PidFile
from ox_dw_logger import get_etl_logger
from ox_dw_odfi_client import (NoDataSetException, DataSizeMismatchException,
                               MD5MismatchException)
from ..common.job import Job
from ..common.settings import ENV, LOCK_ROOT


def acquire_lock(name):
    """
    Attempts to acquire a lock for the name provided.
    """
    lock = PidFile(
        pidname="%s.LOCK" % name,
        piddir=LOCK_ROOT,
        enforce_dotpid_postfix=False)
    lock.create()


def get_logger(name, debug=False):
    """
    Returns a logger with the appropriate log level.
    """
    logger = get_etl_logger(
        name,
        log_level=logging.DEBUG
        if debug else getattr(logging, ENV.get('LOG_LEVEL', 'INFO')))
    if sys.stdin.isatty():
        sys.stderr.write("Logging to %s\n" % logger.handlers[0].baseFilename)

    return logger


def _loader(name, job_name, classname, options, dbh):
    """
    This is a wrapper for each the downloader and uploader since they are so
    similar.
    """
    job = Job(job_name,  dbh, get_logger(name, debug=options.debug),options)
    try:
        job.logger.info("\n\n===================")
        job.logger.info("Starting %s.", name)
        has_data = classname(job)()
        job.logger.info("Finished %s.", name)
        return has_data
    except NoDataSetException as error:
        job.logger.warning(error)
        return False
    except (IOError, DataSizeMismatchException, MD5MismatchException) as error:
        job.logger.warning(
            "Partfiles are corrupt or missing! JOB_NAME:%s; "
            "FEED_NAME:%s; READABLE_INTERVAL:%s; ODFI not ready. %s", job.name,
            job.feed_name, job.load_state.variable_value, error)
        return False
    except Exception as error:
        job.logger.error("Unhandled exception: %s", str(error))
