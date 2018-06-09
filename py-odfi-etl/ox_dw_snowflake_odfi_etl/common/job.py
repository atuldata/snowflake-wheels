"""
An individual job from config.
"""
from dateutil.parser import parse as parse_date
from ox_dw_load_state import LoadState
from ox_dw_odfi_client import (Feeds, get_format_by_freq,
                               get_next_readable_interval_by_freq)
from .exceptions import JobNotFoundException
from .settings import get_conf, ODFI_CONF


class Job(object):
    """
    Determines if a job is ready to execute.
    The properties are not cached so load_state updates are used immediately.
    :param name: Job Name
    :param dbh: database connection
    :param logger: logger object to use
    """

    def __init__(self, name, dbh, logger, options=None):
        self.name = name
        self.dbh = dbh
        self.logger = logger
        self.config = get_conf(self.name)
        self.feed = Feeds(ODFI_CONF)[self.feed_name]
        self.load_state = LoadState(
            self.dbh, variable_name=self.config['LOAD_STATE_VAR'])
        self.options = options
        self._depends_on = None


    @property
    def depends_on(self):
        """
        Returns a list of dependencies as Job objects.
        """
        if self._depends_on is None:
            self._depends_on = [
                Job(job_name, self.dbh, self.logger)
                for job_name in self.config.get('DEPENDS_ON', [])
            ]

        return self._depends_on

    @property
    def feed_name(self):
        """
        The name of the feed for this job.
        """
        return self.config['FEED_NAME']

    @property
    def next_interval(self):
        """
        Return the next interval to be loaded for this job.
        """
        return get_next_readable_interval_by_freq(
            self.readable_interval, self.feed.meta.get('frequency'))

    @property
    def readable_interval(self):
        """
        Returns a datetime object of the load state variable value.
        """
        if self.load_state.variable_value is None:
            raise JobNotFoundException(self.name)

        return parse_date(self.load_state.variable_value)

    @property
    def readable_interval_str(self):
        """
        Returns the current load_state formatted for the interval.
        """
        return self.readable_interval.strftime(
            get_format_by_freq(self.feed.meta.get('frequency')))

    @property
    def rollup_start_date(self):
        return self.options.rollup_start_date

    @property
    def rollup_end_date(self):
        return self.options.rollup_end_date

    @property
    def rollup_interval_type(self):
        return self.options.rollup_interval_type

    @property
    def preview_rollup_queue(self):
        return self.options.preview_rollup_queue

    @property
    def run_rollup_queue(self):
        return self.options.run_rollup_queue

    @property
    def rollup_name(self):
        return self.options.rollup_name

    def get_current_dataset(self):
        """
        Returns the current dataset for this load_state.
        """
        for dataset in self.feed.get_datasets(
                args={
                    'interval': self.readable_interval_str
                }):
            return dataset
