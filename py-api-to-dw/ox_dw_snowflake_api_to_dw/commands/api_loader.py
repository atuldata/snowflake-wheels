"""
Command line scripts for the api_loader here.
"""
import sys
import textwrap
import pid
from .common import get_parser
from ..loader import APILoader
from ..settings import LOADER_APP_NAME


def api_loader():
    """
    This will attempt to load all of the output files in the output dirs.
    This will continue to run monitoring for new files forever.
    Good idea to detach this process if started from a terminal.
    """
    parser = get_parser(api_loader.__doc__)
    parser.add_argument(
        'tables',
        nargs='*',
        help=textwrap.dedent("""
            The list of tables to load for.
            Defaults to all in the output dirs."""))

    options = parser.parse_args()

    api_job_loader = APILoader(options.schema_file, options.tables)
    try:
        api_job_loader.lock.create()
        api_job_loader.start_loader()
    except (pid.PidFileAlreadyLockedError, pid.PidFileAlreadyRunningError):
        api_job_loader.logger.warning("Unable to get lock for application %s. Exiting...",
                                      LOADER_APP_NAME)
