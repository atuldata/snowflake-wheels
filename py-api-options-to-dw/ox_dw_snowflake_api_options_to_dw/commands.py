"""
Command line tools here.
"""
import argparse
import textwrap
import yaml
from .dw_loader import DWLoader
from .settings import DEFAULT_CONFIG_FILE


def api_options_to_dw():
    """
    This will load table(s) from api options into the DW.
    Required is a config file.
    Default table list will be all in the config.
    """
    parser = argparse.ArgumentParser(description=api_options_to_dw.__doc__)
    parser.add_argument(
        '-c', '--config',
        default=DEFAULT_CONFIG_FILE,
        type=argparse.FileType('r'),
        help=textwrap.dedent("""
            YAML file with the tables configuration.
            Default is %s
        """ % DEFAULT_CONFIG_FILE))
    parser.add_argument(
        'tables',
        nargs='*',
        help="List of tables to load."
    )
    options = parser.parse_args()

    DWLoader(yaml.load(options.config), tables=options.tables).run()
