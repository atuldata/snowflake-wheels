"""
Command line scripts for the api_consumer here.
"""
import sys
import textwrap
import pid
from .common import get_parser
from ..client import get_api_client


def api_consumer():
    """
    This is the daemonized process that will get messages from our in-house
    API based on the config file given as an argument.
    """
    parser = get_parser(api_consumer.__doc__)
    parser.add_argument(
        'config_file',
        nargs='?',
        help=textwrap.dedent("""
            The configuration yaml file with the rabbitmq credentials,
            parameters and queues."""))

    options = parser.parse_args()

    if not options.config_file:
        parser.print_usage()
        sys.exit(0)

    client = get_api_client(options.config_file, options.schema_file)
    try:
        client.lock.create()
        client.start_consumer()
    except (pid.PidFileAlreadyLockedError, pid.PidFileAlreadyRunningError):
        for logger in [sys.stderr.write, client.logger.warning]:
            logger("Unable to get lock file. Exiting...")
