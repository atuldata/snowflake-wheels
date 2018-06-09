"""
Runs for a list of salesforce objects.
"""
import logging
import time
import pid
from simple_salesforce import Salesforce

from ox_dw_load_state import LoadState

from .salesforce_object import SalesForceObject
from .settings import (CONFIG, LOGGER, APP_NAME, TRUNCATE_MONITOR_TABLE, ENV,
                       LOCK_ROOT)


class SalesForceObjects(object):
    """
    Runs for all configured objects.
    """

    def __init__(self, oxdb, object_names=ENV['SF_OBJECT_NAMES'], debug=False):
        self.object_names = object_names
        if debug:
            LOGGER.setLevel(logging.DEBUG)
        self.oxdb = oxdb
        self.client = \
            Salesforce(
                username=ENV["SF_USERNAME"],
                password=ENV["SF_PASSWORD"],
                security_token=ENV["SF_SECURITY_TOKEN"],
                sandbox=ENV["SF_IS_SANDBOX"],
                proxies=ENV["SF_PROXIES"],
                version=ENV["SF_API_VERSION"])

    def main(self):
        """
        This is the main runner for all of the configured SalesForce
        objects to run.
        """
        try:
            with pid.PidFile(
                    pidname="%s.LOCK" % APP_NAME,
                    piddir=LOCK_ROOT,
                    enforce_dotpid_postfix=False) as p_lock:
                LOGGER.info(
                    "##################################################")
                LOGGER.info(
                    "########### START OF SALESFORCE PULL #############")
                LOGGER.info(
                    "##################################################")
                LOGGER.info("Running salesforce pull with process id :%d",
                            p_lock.pid)

                self.truncate_monitor_table()

                for object_name in self.object_names:
                    start_time = time.time()
                    LOGGER.info(
                        ">>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>")
                    LOGGER.info("Starting %s...", object_name)
                    try:
                        SalesForceObject(object_name, self.client,
                                         self.oxdb).main()
                    except Exception as error:
                        LOGGER.error(error)
                        raise
                    LOGGER.info("Finished %s in %s seconds", object_name,
                                (time.time() - start_time))
                    LOGGER.info(
                        "<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<")
                self.update_load_state()
        except (pid.PidFileAlreadyRunningError, pid.PidFileAlreadyLockedError):
            LOGGER.warning("Unable to get the lock. Exiting...")
        except Exception as error:
            LOGGER.error("Salesforce load job FAILED. ERROR %s", error)
            raise

    def truncate_monitor_table(self):
        """
        TODO: What is this monitor table for?
        """
        LOGGER.info("Starting truncate_monitor_table...")
        LOGGER.debug(TRUNCATE_MONITOR_TABLE)
        self.oxdb.execute(TRUNCATE_MONITOR_TABLE, commit=True)
        LOGGER.info("Finished truncate_monitor_table")

    def update_load_state(self):
        """
        Update load_state and commit.
        """
        LoadState(self.oxdb.connection,
                  CONFIG['LOAD_STATE_VAR']).upsert(commit=True)
        LOGGER.info("Updated and Committed load_state variable for %s",
                    CONFIG['LOAD_STATE_VAR'])
