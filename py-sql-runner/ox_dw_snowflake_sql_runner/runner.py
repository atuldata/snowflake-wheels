"""
Runs a series of statements against the configured dw_name.
"""
import csv
import sys
import pid
from ox_dw_db import OXDB
from ox_dw_logger import get_etl_logger
from ox_dw_load_state import LoadState
from .settings import DEFAULT_FIELD_SEP, APP_NAME, LOCK_ROOT


def run(config):
    """
    Required in the config are:
        STATEMENTS: Ordered list of SQL statements to run.
    Optional:
        DW_NAME: Data warehouse name.
        Required either in config file or sql_runner argument, -d (--dw_name).
        APP_NAME: This is used for the logger.
              Defaults to LOAD_STATE_VAR or sql_runner
        LOAD_STATE_VAR: If present will update this load state var.
    This will fail at the first statement that fails and will not continue.
    Be sure the use local temporary or temporary tables as there is no clean up.
    """
    job_name = config.get('APP_NAME', APP_NAME)
    logger = get_etl_logger(job_name)
    try:
        with pid.PidFile(pidname="%s.LOCK" % job_name,
                         piddir=LOCK_ROOT, enforce_dotpid_postfix=False) as p_lock:
            logger.info("-------------------------------")
            logger.info("Running %s application with process id: %d", job_name, p_lock.pid)
            logger.info("Starting %s for load_state_variable %s", job_name, config.get('LOAD_STATE_VAR'))
            if sys.stdout.isatty():
                sys.stderr.write(
                    "Logging all output to %s\n" %
                    logger.handlers[0].baseFilename)
            logger.info("Connecting to %s", config.get('DW_NAME'))
            with OXDB(config.get('DW_NAME')) as oxdb:
                size = len(config.get('STATEMENTS'))
                # Set dynamic variables
                for key, val in config.get('VARIABLES').items():
                    if str(val).lower().startswith('select '):
                        val %= config.get('VARIABLES')
                        config['VARIABLES'][key], = \
                            oxdb.get_executed_cursor(val).fetchone()
                for index, statement in enumerate(config.get('STATEMENTS'), start=1):
                    statement %= config.get('VARIABLES')
                    logger.info("STATEMENT(%s/%s) %s;", index, size, statement)
                    cursor = oxdb.get_executed_cursor(statement)
                    if str(statement).lower().startswith('select '):
                        writer = \
                            csv.writer(
                                sys.stdout,
                                delimiter=config.get('FIELD_SEP', DEFAULT_FIELD_SEP))
                        if config.get('HEADERS', False):
                            writer.writerow(col[0] for col in cursor.description)
                        for row in cursor:
                            writer.writerow(row)
                    else:
                        cursor.execute(statement)
                if config.get('LOAD_STATE_VAR') is not None:
                    logger.info(
                        "SETTING %s in load_state.", config.get('LOAD_STATE_VAR'))
                    LoadState(
                        oxdb.connection, variable_name=config.get('LOAD_STATE_VAR')
                    ).upsert()
            logger.info("Completed %s for load_state_variable %s", job_name, config.get('LOAD_STATE_VAR'))
    except (pid.PidFileAlreadyRunningError, pid.PidFileAlreadyLockedError):
        logger.warning("Unable to get lock for %s application. Exiting...", job_name)
    except Exception as err:
        logger.error("Application %s FAILED. ERROR %s", job_name, err)
        raise Exception("Application %s FAILED. ERROR %s" % (job_name, err))
