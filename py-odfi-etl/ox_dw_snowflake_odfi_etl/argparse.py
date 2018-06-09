"""
Customized Argparser for sub-actions.
"""
import argparse
import ntpath
import sys
from ox_dw_odfi_client import valid_readable_interval, FREQUENCIES_BY_FORMAT
from .actions import ACTIONS

OPTIONS = {
    "action": {
        "nargs": 1,
        "help": "This is what we are going to do."
    },
    "debug": {
        "action": "store_true",
        "default": False,
        "help": "Set log level to DEBUG"
    },
    "end_serial": {
        "type":
        int,
        "help":
        "The end of the range of serials to run for. "
        "If left out this will run until the last available serial in ODFI."
    },
    "job_name": {
        "required": True,
        "help": "Name of the job to run."
    },
    "readable_interval_str": {
        "type":
        valid_readable_interval,
        "required":
        True,
        "help":
        "The readable_interval_str to run for. Should be formatted one "
        "of the following: %s" % [
            interval_format.replace('%', '%%')
            for interval_format in FREQUENCIES_BY_FORMAT
        ]
    },
    "start_serial": {
        "type":
        int,
        "help":
        "The start of the range of serials to run for. Will default "
        "to the greatest serial the uploader is at."
    },
    "type": {
        "default": "delta",
        "help": "Report Type"
    },
    "rollup_name": {
        "default": None,
        "help": "use if you want to run particular rollup based of it's name for example ROLLUP_ox_transaction_sum"
    },
    "rollup_start_date": {
        "default": None,
        "help": "rollup start date the rollup job will queue all the posibile combination for the rollup start date"
    },
    "rollup_end_date": {
            "default": None,
            "help": "this is when rolllup ends"
    },
    "rollup_interval_type": {
            "default": None,
            "help": "rollup interval type such as day, month, advertiser_day"
    },
    "preview_rollup_queue": {
            "default": None,
            "help": "Preview Rollup Queue, it will just log all the information from the rollup_queue for the job"
    },
    "run_rollup_queue": {
            "default": None,
            "help": "Run rollup from the queue, to run rollup you can provide this by default it is true"
    }
}

class _HelpAction(argparse._HelpAction):
    def __call__(self, parser, namespace, values, option_string=None):
        if hasattr(namespace, 'action') and namespace.action is not None:
            subparsers_actions = [
                action for action in parser._actions
                if isinstance(action, argparse._SubParsersAction)
            ]
            for subparsers_action in subparsers_actions:
                # get all subparsers and print help
                for choice, subparser in subparsers_action.choices.items():
                    if choice in namespace.action:
                        print(subparser.format_help())
        else:
            parser.print_help()

        parser.exit()


def get_parsers(description):
    """
    Returns parent and actions parsers.
    """
    usage = "%s {%s} [-h|--help]" % (ntpath.basename(sys.argv[0]),
                                     ','.join(sorted(list(ACTIONS.keys()))))
    parent = \
        argparse.ArgumentParser(
            description=description,
            formatter_class=argparse.ArgumentDefaultsHelpFormatter,
            add_help=False,
            usage=usage)
    parent.add_argument(
        'action',
        nargs=1,
        choices=sorted(list(ACTIONS.keys())),
        help="What are we doing today? For help on an action use: "
        "%s <action> -h|--help"
        "" % ntpath.basename(sys.argv[0]))

    taken_letters = set(list('h'))
    for option in sorted(list(OPTIONS.keys())):
        if option == 'action':
            continue
        flags = []
        if option[0] not in taken_letters:
            flags.append("-%s" % option[0])
            taken_letters.add(option[0])
        flags.append("--%s" % option)
        options = dict(
            (key, value) for key, value in OPTIONS.get(option).items()
            if key != 'required')
        parent.add_argument(*flags, **options)
    parent.add_argument(
        '-h',
        '--help',
        help="show this help message and exit",
        action=_HelpAction)

    # Setup action sub-parsers
    actions = parent.add_subparsers(title='Actions')
    for action_name, config in sorted(ACTIONS.items()):
        actions.add_parser(
            action_name,
            help=config.get('help'),
            prog="%s %s" % (ntpath.basename(sys.argv[0]), action_name),
            formatter_class=argparse.ArgumentDefaultsHelpFormatter)
        taken_letters = set(list('h'))
        for option in config.get('options', []):
            flags = []
            if 'nargs' in OPTIONS.get(option):
                flags.append(option)
            else:
                if option[0] not in taken_letters:
                    flags.append("-%s" % option[0])
                    taken_letters.add(option[0])
                flags.append("--%s" % option)
            actions.choices.get(action_name).add_argument(
                *flags, **OPTIONS.get(option))

    return parent, actions
