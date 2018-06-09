"""
Command line function.
"""
from argparse import ArgumentParser, RawTextHelpFormatter
from dateutil.parser import parse as parse_date
import pid
from .loader import Loader
from .settings import END_DATE


def main():
    """
    The script pulls data from oanda site and loads it into
    currency_exchange_daily_fact. Defaults to running for latest date in
    currency_exchange_daily_fact to the current date.
    """
    parser = \
        ArgumentParser(
            description=main.__doc__.lstrip(),
            formatter_class=RawTextHelpFormatter)
    parser.add_argument(
        '-s', '--start_date',
        type=parse_date,
        help="To pull currency exchange since the given start date.")
    parser.add_argument(
        "-e", "--end_date",
        type=parse_date,
        default=END_DATE,
        help="To pull currency exchange up to the given end date.")

    options = parser.parse_args()

    try:
        loader_obj = Loader(options.start_date, options.end_date)
        loader_obj.lock.create()
        loader_obj.run()
    except (pid.PidFileAlreadyRunningError, pid.PidFileAlreadyLockedError):
        loader_obj.logger.warning("Unable to acquire lock.Exiting... ")
    except Exception as error:
        raise Exception("Currency rate loader failed.ERROR:%s" % error)
