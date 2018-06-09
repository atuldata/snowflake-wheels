"""
Common to command line scripts goes here.
"""
import argparse
from ..settings import DEFAULT_SCHEMA_FILE


def get_parser(description):
    """
    Return a parser with the common arguments.
    """
    parser = \
        argparse.ArgumentParser(
            description=description, allow_abbrev=True,
            formatter_class=argparse.ArgumentDefaultsHelpFormatter)
    parser.add_argument(
        '-s', '--schema_file',
        nargs='?',
        default=DEFAULT_SCHEMA_FILE,
        help="This is optional.")

    return parser
