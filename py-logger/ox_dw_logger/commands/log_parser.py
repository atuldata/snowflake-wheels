"""
Command line functions here.
"""
import argparse
import textwrap
from ..log_parser import TAGS, email_report, print_report
from ..settings import ENV


def etl_logs_email_report():
    """
    Emails html report of the new messages since the last-run
    """
    parser = argparse.ArgumentParser(description=etl_logs_email_report.__doc__)
    parser.add_argument(
        'log_files',
        nargs='*',
        help=textwrap.dedent("""
            List of log files to parse.
            Default looks for all *.log files recursively in the log_root."""))
    parser.add_argument(
        '--tag',
        default='ERROR',
        choices=TAGS,
        help=textwrap.dedent("""
            Which tag do you want reported?
            Default is ERROR."""))
    parser.add_argument(
        '--email',
        default=ENV.get('CHECK_ERROR_EMAIL_RECIPIENTS'),
        help=textwrap.dedent("""
            Who to send the email to? Defaults to %s"""
                             % ENV.get('CHECK_ERROR_EMAIL_RECIPIENTS')))

    options = parser.parse_args()

    email_report(
        options.tag,
        options.email,
        log_files=options.log_files)


def etl_logs_print_report():
    """
    Prints table to stdout of the new messages since the last n hours.
    """
    parser = argparse.ArgumentParser(description=etl_logs_print_report.__doc__)
    parser.add_argument(
        'log_files',
        nargs='*',
        help=textwrap.dedent("""
            List of log files to parse.
            Default looks for all *.log files recursively in the log_root."""))
    parser.add_argument(
        '--tag',
        default='ERROR',
        choices=TAGS,
        help=textwrap.dedent("""
            Which tag do you want reported?
            Default is ERROR."""))
    parser.add_argument(
        '--hours',
        default=1,
        type=int,
        help=textwrap.dedent("""
            How many hours back to dig up the tags?
            Default is 1."""))
    parser.add_argument(
        '--html',
        action="store_true",
        help=textwrap.dedent("""
            Output in HTML?
            Default will be a pretty ascii table output."""))

    options = parser.parse_args()

    print_report(
        options.tag,
        options.log_files,
        hours=options.hours,
        html=options.html)
