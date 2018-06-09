"""
Code for working with Readable Intervals.
"""
from datetime import datetime, timedelta

FREQUENCIES_BY_FORMAT = {"%Y-%m-%d": "DAILY", "%Y-%m-%d_%H": "HOURLY"}
INTERVAL_BY_FREQUENCY = {"DAILY": "days", "HOURLY": "hours"}


def get_format_by_freq(frequency):
    """
    Returns the formatting for the given frequency.
    """
    for interval_format, interval_frequency in FREQUENCIES_BY_FORMAT.items():
        if interval_frequency == frequency:
            return interval_format


def get_frequency(in_str):
    """
    Return the appropriate frequency for the date format str
    :param in_str: The readable_interval as a string.
    """
    return FREQUENCIES_BY_FORMAT.get(get_interval_format(in_str))


def get_interval(in_str):
    """
    Returns the appropriate python interval for the date str.
    """
    return INTERVAL_BY_FREQUENCY.get(get_frequency(in_str))


def get_interval_format(in_str):
    """
    Return the format for a given date str.
    Needs to be in FREQUENCIES_BY_FORMAT.
    :param in_str: The readable_interval as a string.
    """
    for interval_format in FREQUENCIES_BY_FORMAT:
        try:
            if datetime.strptime(in_str, interval_format):
                return interval_format
        except ValueError:
            pass  # Pass until we have tried all formats.
    raise ValueError(
        "Readable_interval:%s; does not match known formats!" % in_str)


def get_next_readable_interval(in_datetime, interval):
    """
    Returns the next readable interval as a datatime object
    for the given interval.
    :param in_datetime: The readable_interval as a datetime object.
    :param interval: The python interval to use as the delta(hours, days)
    """
    kwargs = {interval: 1}
    return in_datetime + timedelta(**kwargs)


def get_next_readable_interval_by_freq(in_datetime, frequency):
    """
    Returns the next readable interval as a datatime object
    for the given interval.
    :param in_datetime: The readable_interval as a datetime object.
    :param frequency: odfi feed frequency to use as the delta(hours, days)
    """
    kwargs = {INTERVAL_BY_FREQUENCY.get(frequency): 1}
    return in_datetime + timedelta(**kwargs)


def readable_interval_datetime(in_str):
    """
    Return a datetime object.
    :param in_str: The readable_interval as a string.
    """
    return datetime.strptime(in_str, get_interval_format(in_str))


def readable_interval_str(in_datetime, interval_format):
    """
    Returns a string.
    :param in_datetime: datetime.datetime
    :param interval_format: Date formating.
    """
    return in_datetime.strftime(interval_format)


def valid_readable_interval(in_str):
    """
    Returns the in_str if is is valid.
    Raises ValueError if not.
    """
    if get_frequency(in_str):
        return in_str
    raise ValueError("%s is not a valid format!!!" % in_str)
