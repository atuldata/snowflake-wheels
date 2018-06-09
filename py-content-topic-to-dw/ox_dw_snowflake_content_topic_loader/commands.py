"""
Command line function.
"""
from argparse import ArgumentParser, RawTextHelpFormatter
from .loader import Loader


def main():
    """
    The script reads in the content_topics.json files for each customer
    and loads into the DW.
    """
    parser = \
        ArgumentParser(
            description=main.__doc__.lstrip(),
            formatter_class=RawTextHelpFormatter)

    _ = parser.parse_args()

    Loader().run()
