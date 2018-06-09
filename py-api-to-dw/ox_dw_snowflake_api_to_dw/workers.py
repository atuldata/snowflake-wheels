"""
Add your custom workers/consumers here.
"""
from datetime import datetime, timedelta

from ox_rabbit_mq import Worker
from .signal_trap import ExitSignalTrap
from .extractor import Extractor
from .settings import FLUSH_PERIOD


class APIExtractorWorker(Worker):
    """
    Our custom worker for use with RabbitMQ.
    This will act as a consumer. This will periodically flush the consumed
    data based on FLUSH_PERIOD minutes.
    """

    _extractor = None
    _last_flush_time = None

    def __init__(self, name, config, logger):
        super(APIExtractorWorker, self).__init__(name, config, logger)
        ExitSignalTrap(self.close)

    @property
    def extractor(self):
        """
        This is where the processing takes place.
        """
        if self._extractor is None:
            self._extractor = \
                Extractor(self.name, self.config, self.logger)

        return self._extractor

    @property
    def last_flush_time(self):
        """
        Returns the last time the flush was set.
        """
        if self._last_flush_time is None:
            self.last_flush_time = datetime.now()

        return self._last_flush_time

    @last_flush_time.setter
    def last_flush_time(self, value):
        self._last_flush_time = value

    def close(self):
        """
        Upon exiting be sure to flush anything that has been consumed.
        """
        self.logger.debug("Calling flush_table_data() just before exiting.")
        self.extractor.flush_table_data()

    def is_flush_time(self):
        """
        The FLUSH_PERIOD is a setting in minutes. This lets us know that
        FLUSH_PERIOD minutes have past.
        """
        return \
            self.last_flush_time + timedelta(minutes=FLUSH_PERIOD) \
            < datetime.now()

    # overrides
    def on_timeout(self):
        """
        The consumers timeout calls this so we can periodically export the
        extracted data to files for the loader.
        """
        if self.is_flush_time():
            self.extractor.flush_table_data()
            self.reset_flush_time()

    # overrides
    def process(self, body_obj):
        """
        Stores each object for a later batch load into Vertica.
        """
        action = body_obj.get('action', 'null').lower()
        self.logger.debug(
            "Processing object of TYPE=%s, ID=%s, UID='%s' ACTION=%s",
            body_obj.get('object', {}).get('type'),
            body_obj.get('object', {}).get('id'),
            body_obj.get('object', {}).get('uid'),
            action)
        try:
            if action in self.config.action_types:
                self.extractor.update_handler(body_obj)
        except Exception as error:
            self.logger.error(error)
            raise

        self.on_timeout()

        # Empty message_dict
        return {}

    def reset_flush_time(self):
        """
        Unsets the last_flush_time variable.
        """
        self.last_flush_time = None

    __del__ = __exit__ = close
