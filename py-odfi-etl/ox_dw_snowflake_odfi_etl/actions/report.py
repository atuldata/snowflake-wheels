"""
For reporting statuses
"""
import os
import logging
from datetime import datetime, timedelta
from prettytable import PrettyTable, ALL
from ox_dw_db import OXDB
from .actors.downloader import Downloader
from ..common.exceptions import JobNotFoundException
from ..common.settings import JOB_CONF_ROOT
from ..common.job import Job

OPTIONS = ["type"]
REPORT_TYPES = ("delta", )


def report(options, dbh):
    """
    Report on the status of the ETL Jobs.
    """
    if options.type == 'delta':
        delta(dbh)


def delta(dbh):
    """
    Prints out the delta report.
    """
    start = datetime.utcnow()
    now = start.replace(minute=0, second=0, microsecond=0) - timedelta(hours=1)
    table = PrettyTable(hrules=ALL)
    table.field_names = [
        "Job Name(Feed Name)/TrueUp Delta", "ODFI Interval(Serial)/Delta",
        "Download Interval(Serial)/Delta", "Upload Interval(Serial)/Delta"
    ]
    table.align = 'l'
    for field_name in table.field_names:
        if 'Delta' in field_name:
            table.align[field_name] = 'r'
    table.header = True

    with OXDB('SNOWFLAKE') as dbh:
        logger = logging.getLogger('DUMMY')
        logger.setLevel(logging.CRITICAL)
        for job_file in sorted(os.listdir(JOB_CONF_ROOT)):
            if not job_file.endswith('yaml'):
                continue
            job = Job(os.path.splitext(job_file)[0], dbh.connection, logger)
            row = []
            try:
                download_dataset = job.feed.get_dataset_by_serial(
                    Downloader(job).get_since_serial())
                upload_dataset = job.get_current_dataset()
                odfi_dataset = job.feed.latest_dataset
                row.extend([
                    "%s(%s)\n%s" %
                    (job.name, job.feed.name,
                     str(now - upload_dataset.readable_interval).replace(
                         ':00:00', ' hours')),
                    "%s(%s)\n%s" %
                    (odfi_dataset.readable_interval, odfi_dataset.serial,
                     str(now - odfi_dataset.readable_interval).replace(
                         ':00:00', ' hours')),
                    "%s(%s)\n%s" %
                    (download_dataset.readable_interval,
                     download_dataset.serial,
                     str(odfi_dataset.readable_interval -
                         download_dataset.readable_interval).replace(
                             ':00:00', ' hours')),
                    "%s(%s)\n%s" %
                    (upload_dataset.readable_interval, upload_dataset.serial,
                     str(download_dataset.readable_interval -
                         upload_dataset.readable_interval).replace(
                             ':00:00', ' hours'))
                ])
            except JobNotFoundException:
                row.extend([
                    "%s(%s)\n%s" %
                    (job.name, job.feed.name,
                     str(now - now).replace(':00:00', ' hours')),
                    "None(None)\n%s" % str(now - now).replace(
                        ':00:00', ' hours'),
                    "None(None)\n%s" % str(now - now).replace(
                        ':00:00', ' hours'),
                    "None(None)\n%s" % str(now - now).replace(
                        ':00:00', ' hours')
                ])
            table.add_row(row)
    print("Report Time: %s" % now)
    print(table)
    print("Report Duration: %s" % (datetime.utcnow() - start))
