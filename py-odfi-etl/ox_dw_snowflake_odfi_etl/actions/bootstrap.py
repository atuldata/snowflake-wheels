"""
Sets up a new job to the system. The readable_interval you input is the
 'since' so be careful when adding as these values will not be run.
"""
from datetime import datetime
import sys
from ox_dw_load_state import LoadState
from ox_dw_odfi_client import readable_interval_datetime, NoDataSetException
from ._base import get_logger
from .actors.downloader import Downloader
from .actors.uploader import Uploader
from ..common.job import Job

OPTIONS = ["job_name", "readable_interval_str", "debug"]
NAME = 'odfi_etl_bootstrap'
DELETE_DOWNLOAD = """
DELETE FROM odfi_etl_download_status
WHERE dataset_serial >= ?"""


def bootstrap(options, dbh):
    """
    Before you can run you need to initialize the load_state for a given
    job_name/feed_name.
    :param options: NamedTuple including options defined for this action in
                    .actions.py
    """
    job = Job(options.job_name, dbh, get_logger(NAME, debug=options.debug))
    proceed = 'n'
    if job.load_state.variable_value:
        while True:
            proceed = str(
                input(
                    "%s already exists with value %s. "
                    "Okay to Update to new values?(Y/n) "
                    % (job.load_state.variable_name,
                       job.load_state.variable_value))).lower() or 'y'
            if proceed in ('y', 'n'):
                if proceed == 'n':
                    sys.exit(0)
                break

    now = datetime.utcnow()

    downloader = Downloader(job)
    uploader = Uploader(job)

    # Create if not exists stage and status table
    downloader.create_stage()
    downloader.create_status_table()
    uploader.create_status_table()

    # Add new data to the DB
    try:
        min_dataset = job.feed.get_min_dataset(options.readable_interval_str)
        job.load_state.upsert(
            readable_interval_datetime(options.readable_interval_str))
        downloader.add_status(min_dataset, now)
        LoadState(job.dbh,
                  uploader.load_state_serial_name).upsert(min_dataset.serial)

        action = 'Updated' if proceed == 'y' else 'Added'
        sys.stderr.write(
            "%s load_state:%s;%s\n" % (action, job.load_state.variable_name,
                                       job.load_state.variable_value))
    except NoDataSetException as error:
        job.logger.error(error)
        print('ERROR', error)
