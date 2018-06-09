"""
Command line scripts for api_harmonizer here.
"""
import sys
from datetime import datetime, timedelta
import textwrap
from dateutil.parser import parse as parse_date
from .common import get_parser
from ..harmonizer import Harmonizer


def api_harmonizer():
    """
    For use when there are items missing in the DW that are in the API.
    Will query the API DB directly and create the output files just as the
    api_consumer does. The counter on the console is only approximate count.
    Look in the logs for the exact count written.
    """
    parser = get_parser(api_harmonizer.__doc__)
    parser.add_argument(
        'object_type',
        nargs='?',
        help="The API object_type to pull the data from")
    parser.add_argument(
        'start_time',
        nargs='?',
        type=parse_date,
        default=datetime.utcnow()-timedelta(days=1),
        help="The start of the modified_date range to harmonize for.")
    parser.add_argument(
        'end_time',
        nargs='?',
        type=parse_date,
        default=datetime.utcnow(),
        help=textwrap.dedent("""
            The end of the modified_date range to harmonize for."""))
    parser.add_argument(
        '-w', '--where_clause',
        help=textwrap.dedent("""
            Example: -w "id = 12345"
            This is optional. Note: No need to add modified_date here since
            the start_time and end_time will be added to the where clause
            as the modified_date range.
        """))

    options = parser.parse_args()

    if options.object_type is None:
        parser.print_usage()
        sys.exit(0)

    Harmonizer(
        options.object_type,
        options.start_time,
        options.end_time,
        where_clause=options.where_clause,
        schema_file=options.schema_file).run()
