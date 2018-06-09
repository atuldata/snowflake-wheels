"""
Loads salesforce objects from Salesforce API
"""
import argparse
import textwrap
from ox_dw_db import OXDB
from .salesforce_objects import SalesForceObjects
from .settings import ENV


def import_objects():
    """
    This is the main runner for all of the configured SalesForce
    objects to run.
    """
    parser = \
        argparse.ArgumentParser(description=import_objects.__doc__)
    parser.add_argument(
        'object_names',
        nargs='*',
        default=ENV['SF_OBJECT_NAMES'],
        help=textwrap.dedent('''
            Optionally you can specify a list of salesforce objects
            to run for. Defaults to %s.''' % ENV['SF_OBJECT_NAMES']))
    parser.add_argument(
        '-d', '--debug', action="store_true", help="Set log level to debug")
    options = parser.parse_args()

    with OXDB('SNOWFLAKE') as oxdb:
        SalesForceObjects(
            oxdb, object_names=options.object_names,
            debug=options.debug).main()
