"""
Ensure that all datset partfiles are indeed staged for Snowflake.
Will download/put any missing partfiles.
"""
from ox_dw_odfi_client import NoDataSetException
from ._base import acquire_lock, get_logger
from .actors.downloader import Downloader
from ..common.job import Job

OPTIONS = ["job_name", "start_serial", "end_serial", "debug"]


def verify_download(options, dbh):
    """
    Ensures that any missing staged part file are staged by puting missing
    datasets to snowflake stage.
    """
    name = '_'.join([options.job_name, 'downloader'])
    acquire_lock(name)
    job = Job(options.job_name, dbh, get_logger(name, debug=options.debug))

    if options.start_serial is None:
        options.start_serial = job.get_current_dataset().serial + 1

    try:
        Downloader(job).download_by_serial_range(options.start_serial,
                                                 options.end_serial)
    except NoDataSetException as error:
        job.logger.warning(error)
