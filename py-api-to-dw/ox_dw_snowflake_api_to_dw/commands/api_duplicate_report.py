"""
For printing and logging found duplicates as errors.
"""
import sys
from progressbar import ProgressBar
from .common import get_parser
from ..loader import APILoader
from ..table_loader import TableLoader


def api_duplicate_report():
    """
    Prints and logs errors for found duplicated in the dimensions.
    """
    parser = get_parser(api_duplicate_report.__doc__)
    options = parser.parse_args()

    loader = APILoader(options.schema_file)
    with ProgressBar(max_value=len(loader.tables)) as bar_:
        for index, table_name in enumerate(sorted(loader.tables)):
            if sys.stdout.isatty():
                bar_.update(index)
            _ = TableLoader(table_name, 'dummy', options.schema_file).has_dupes
