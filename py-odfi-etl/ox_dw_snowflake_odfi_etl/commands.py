"""
Command line functions defined here.
"""
import importlib
import sys
import pid
from ox_dw_db import OXDB
from .argparse import get_parsers
from .common.exceptions import JobNotFoundException


def main():
    """
    This is the entry point for all things odfi_etl related.
    """
    parent, actions = get_parsers(main.__doc__)

    options = parent.parse_args()

    sys.argv.pop(1)

    # Get sub-parser options
    action_options = actions.choices.get(options.action[0]).parse_args()

    # Make the action call
    try:
        with OXDB('SNOWFLAKE') as dbh:
            getattr(
                importlib.import_module(
                    '.actions.%s' % options.action[0],
                    package='ox_dw_snowflake_odfi_etl'),
                options.action[0])(action_options, dbh.connection)
    except pid.PidFileAlreadyLockedError as exception:
        sys.stderr.write("PidLock found! %s; Exiting..." % str(exception))
    except JobNotFoundException as exc:
        sys.stderr.write("%s Please run bootstrap\n" % exc)
        actions.choices.get('bootstrap').print_help()
        sys.exit(1)
