"""
For pulling options json from rest api.
"""
import sys
import pid
try:
    from collections import OrderedDict
except ImportError:
    from ordereddict import OrderedDict
from .table_loader import TableLoader


class DWLoader(object):
    """
    With given config and optional set of tables to load for will call on
    TableLoader for each.
    The tables will load in the order that they are in the config.
    TODO: Add asynchronous mode.
    """

    def __init__(self, config, tables=None):
        self._raw_config = config
        self._tables = tables

        # Need to initialize and transform the config
        self.config = self._get_config()

    @property
    def tables(self):
        """
        List of tables in the order that they will be run.
        """
        if self._tables is None:
            self._tables = []

        return self._tables

    def run(self):
        """
        Loops through the configured tables and load.
        """
        for name, config in self.config.items():
            try:
                loader = TableLoader(name, config)
                loader.lock.create()
                loader.main()
            except (pid.PidFileAlreadyLockedError,
                    pid.PidFileAlreadyRunningError):
                for logger in [sys.stderr.write, loader.logger.warning]:
                    logger(
                        "Lock file %s found and cannot get exclusive lock."
                        "Exiting...", loader.lock.pidname)
            except Exception as error:
                sys.stderr.write("Exiting with error:%s;\n" % error)

    def _get_config(self):
        """
        Transformed config from the yaml to be a dict.
        The raw config is needed as a list so it can provide order.
        """
        config = OrderedDict()
        need_tables = not bool(self.tables)
        for item in self._raw_config:
            for key, value in item.items():
                if need_tables:
                    self.tables.append(key)
                if key in self.tables:
                    config.update({key: value})

        return config
