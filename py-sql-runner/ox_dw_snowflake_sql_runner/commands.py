"""
Command line functions here.
"""
from argparse import ArgumentParser, FileType, RawTextHelpFormatter
import textwrap
import yaml
from .runner import run
from .settings import DEFAULT_FIELD_SEP


def sql_runner():
    """
Requires a yaml config file with a list of statements to run.
Optionally upon success will update the LOAD_STATE_VAR provided.
Example config.yaml:
DW_NAME: SNOWFLAKE
LOAD_STATE_VAR: my_job_loaded
STATEMENTS:
    - CREATE LOCAL TEMPORARY TABLE %(temp_table)s(col1 int, col2 varchar)
      ON COMMIT PRESERVE ROWS
    - INSERT INTO %(temp_table)s SELECT ...
    - COMMIT
VARIABLES:  # Optional. For variable substitution in your STATEMENTS.
    this: that
    foo: bar
FIELD_SEP: ,  # Optional. For outputs
HEADERS: !!bool True  # Optional. If outputting, print the field names
This will log in the etl logger directory as the <load_state_var>.log or
sql_runner.log
This will auto-commit at the end. Only add commit to you STATEMENTS if you
need it somewhere in the middle.
    """
    parser = \
        ArgumentParser(
            description=sql_runner.__doc__.lstrip(),
            formatter_class=RawTextHelpFormatter)
    parser.add_argument(
        'config',
        type=FileType('r'),
        help="YAML file with the configuration.")
    parser.add_argument(
        '-l', '--load_state_var',
        help=textwrap.dedent("""
What load state variable to update upon success?
This is optional as it can be in the config.
If there is no value for this in the config or options then
no load state will be updated.
        """.lstrip()))
    parser.add_argument(
        '-v', '--variables',
        nargs='+',
        action='append',
        help=textwrap.dedent("""
If you have any variable substitutions in your statements add them
here. You can add as many as you like.
These can also be in the config as defaults as VARIABLES:
Format like so: -v this that -v foo bar
Becomes {'this': 'that', 'foo': 'bar'}
        """.lstrip()))
    parser.add_argument(
        '-f', '--field_sep',
        default=DEFAULT_FIELD_SEP,
        help=textwrap.dedent("""
If outputting the field separator to use. Defaults to %s
        """.lstrip() % DEFAULT_FIELD_SEP))
    parser.add_argument(
        '-d', '--dw_name',
        help=textwrap.dedent("""
Which dw_name, data warehouse name, to use. A list of dw_names is available in the env.yaml
        """.lstrip()))
    parser.add_argument(
        '--headers',
        action='store_true',
        help="If outputting, to print out the fieldnames for the first line?")

    options = parser.parse_args()

    config = yaml.load(options.config)

    if 'STATEMENTS' not in config:
        raise \
            ValueError("The STATEMENTS section is missing from the config!")

    if 'DW_NAME' not in config and options.dw_name is None:
        raise \
            ValueError("DW_NAME is required either from the config or input argument!")

    if options.load_state_var is not None:
        config['LOAD_STATE_VAR'] = options.load_state_var

    if config.get('VARIABLES') is None:
        config['VARIABLES'] = {}
    if options.variables is not None:
        config['VARIABLES'].update(options.variables)

    if config.get('FIELD_SEP') is None:
        config['FIELD_SEP'] = options.field_sep

    if config.get('HEADERS') is None:
        config['HEADERS'] = True if options.headers else False

    if options.dw_name is not None:
        config['DW_NAME'] = options.dw_name

    run(config)
