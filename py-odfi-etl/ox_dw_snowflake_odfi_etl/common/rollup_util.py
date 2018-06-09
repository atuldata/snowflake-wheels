
import os
import subprocess
from itertools import repeat, chain
from .settings import APP_CONF_ROOT,  get_conf,APP_ROOT

from ox_dw_load_state import LoadState
CONFIG = get_conf('rollup', APP_CONF_ROOT)
JOB_ENV_FILE = os.path.join(APP_ROOT, 'conf', 'env.yaml')

class RollupJob(object):
    """
    last_hour is the UTC last hour -- platforms where this is the last_hour
    for a given instance_rollup_date will be rolled up.
    """
    def __init__(self, config=None, rollup_type=None, rollup_time_name=None,
                 instance_rollup_date=None, last_hour=None):
        self.config = config
        self.rollup_type = rollup_type
        self.rollup_time_name = rollup_time_name
        self.instance_rollup_date = instance_rollup_date
        self.last_hour = last_hour
        if 'script' in self.config[self.rollup_type]['time_rollups'][self.rollup_time_name]:
            self.is_script = True

    def __str__(self):
        if self.is_script:
            return "Script: %s (%s) will run for time %s" % (
                self.rollup_type, self.rollup_time_name, self.last_hour)
        else:
            return "Rollup: %s (%s) platforms where %s ends rollup date %s" % \
                   (self.rollup_type, self.rollup_time_name, self.last_hour,
                    self.instance_rollup_date)

    # Override __hash__, __eq__ and __cmp__ so that set() handles duplicates.
    def __hash__(self):
        return ((((hash(self.rollup_type) * 31) + hash(
            self.rollup_time_name)) * 31 +
                 hash(self.instance_rollup_date)) * 31) + hash(self.last_hour)

    def __eq__(self, other):
        return self.rollup_type == other.rollup_type and \
               self.rollup_time_name == other.rollup_time_name and \
               self.instance_rollup_date == other.instance_rollup_date and \
               self.last_hour == other.last_hour

    def __cmp__(self, other):
        c = self.rollup_type.cmp(other.rollup_type)
        if c != 0:
            return c
        c = self.rollup_time_name.cmp(other.rollup_time_name)
        if c != 0:
            return c
        c = self.instance_rollup_date.cmp(other.instance_rollup_date)
        if c != 0:
            return c
        return self.last_hour.cmp(other.last_hour)

    @staticmethod
    def process_one(db, rollup_config,rollup_type,logger):
        """
        Run a single job and dequeue it as soon as it completes successfully.
        """
        for (rule_id, rollup_type, rollup_time_name, instance_rollup_date,
            last_hour, rollup_interval_code) in db.cursor().execute(rollup_config['ROLLUP_QUERY_ONE'],(rollup_type,)):
            logger.info("processing %s %s %s" %(rollup_type, rollup_time_name, instance_rollup_date))
            if rule_id is None:
                return None

            rule = \
                RollupJob(
                    config=rollup_config,
                    rollup_type=rollup_type,
                    rollup_time_name=rollup_time_name,
                    instance_rollup_date=instance_rollup_date,
                    last_hour=last_hour)
            run_rollup(db, rollup_config, rollup_type, rollup_time_name,
                       last_hour, True,logger)
            db.cursor().execute(rollup_config['ROLLUP_REMOVE'], (rule_id,))
            db.commit()
            return rule

    @staticmethod
    def save_all(db, rollup_config, jobs):
        """
        Write out all the jobs to a temporary file.
        """
        # Load into a temp table
        db.cursor().execute(rollup_config['ROLLUP_DROP_LOAD_TABLE'])
        db.cursor().execute(rollup_config['ROLLUP_CREATE_LOAD_TABLE'])

        load_sql = rollup_config['ROLLUP_LOAD_TABLE']
        for job in jobs:
            db.cursor().execute(load_sql,
                                (job.rollup_type, job.rollup_time_name, job.instance_rollup_date, job.last_hour))

        # Merge new rollup rows into the table, commit,  and remove the
        # uploaded file
        db.cursor().execute(rollup_config['ROLLUP_MERGE'])
        db.commit()


    @staticmethod
    def load_all(db, rollup_config, rollup_name):
        result = list()
        for row in db.cursor().execute(rollup_config['ROLLUP_QUERY'],(rollup_name,)):
            result.append(
                RollupJob(config=rollup_config,
                          rollup_type=row[0],
                          rollup_time_name=row[1],
                          instance_rollup_date=row[2],
                          last_hour=row[3]))

        return result

def _map_time_param(rollup_date, param):
    if param in ['instance_rollup_date', 'advt_rollup_date', 'utc_rollup_date']:
        return "CAST('%s' AS TIMESTAMP)" % rollup_date
    elif param in ['instance_date_sid', 'advt_date_sid', 'utc_date_sid']:
        return rollup_date.strftime("%Y%m%d")
    elif param in ['instance_month_sid', 'advt_month_sid', 'utc_month_sid']:
        return rollup_date.strftime("%Y%m01")
    else:
        raise Exception("Cannot map type parameter %s" % param)


def _join_affected_sql(time_rollup):
    join_list = \
        ["f.%s=p.%s" % (key, value) for key, value in
         time_rollup['affected'].items()]
    return " AND ".join(join_list)


def _delete_clause_affected_sql(time_rollup, range_start, range_end):
    source_keys = ",".join([x for x in time_rollup['affected'].keys()])
    join_list = ",".join([x for x in time_rollup['affected'].values()])
    where_clause = " WHERE utc_start=CAST('%s' AS TIMESTAMP) and " \
                   "utc_end=CAST('%s' AS TIMESTAMP) " % (
                       range_start, range_end)

    return "(%s) IN ( SELECT %s FROM tmp_affected_rollup %s )" % (
        source_keys, join_list, where_clause)

def get_keys(db, table_name):
    keys=[]
    for row in db.cursor().execute("describe "+table_name):
        if 'date_sid' not in row[0].lower() and 'timestamp' not in row[0].lower() and 'tot_' not in row[0].lower() and 'rollup' not in row[0].lower():
            keys.append(row[0].lower())
    return keys

def get_metrics(db, table_name):
    metrics = []
    for row in db.cursor().execute("describe " + table_name):
        if 'date_sid' not in row[0].lower() and 'timestamp' not in row[0].lower() and 'tot_' in row[0].lower():
            metrics.append(row[0].lower())
    return metrics

def run_rollup_sql(db,rollup_config, rollup_name, time_name, rollup_date,
                   range_start, range_end):
    """
    Produce the SQL to insert all data into the table specified by
    time_rollup, given the last hour of the interval.
    """
    table_config = rollup_config['ROLLUP_CONFIG'][rollup_name]
    time_rollup = table_config['time_rollups'][time_name]


    if 'keys' not in table_config:
        rollup_keys = get_keys(db, time_rollup['table'])
    else:
        rollup_keys = table_config['keys']

    if 'metrics' not in table_config:
        rollup_metrics=get_metrics(db,time_rollup['table'])
    else:
        rollup_metrics=table_config['metrics']

    sql = "INSERT /*+ direct */ INTO %s (" % (time_rollup['table'])
    sql += "  %s," % (", ".join(time_rollup['keys']))
    sql += "  %s," % (", ".join(rollup_keys))
    sql += "  %s )\n" % (", ".join(rollup_metrics))
    sql += "SELECT "

    time_param_list = [_map_time_param(rollup_date, x) for x in
                       time_rollup['keys']]
    sql += "  %s," % (", ".join(time_param_list))

    sql += "  f.%s," % (", f.".join(rollup_keys))
    sql += "  sum(%s)\n" % ("), sum(".join(rollup_metrics))
    if table_config['timezone'].upper() == 'UTC' \
            and time_rollup['timezone'].upper() == 'UTC':
        sql += "FROM %s f " % (time_rollup['source'])
    else:
        sql += "FROM %s f INNER JOIN tmp_affected_rollup p ON (" % (
            time_rollup['source'])
        sql += _join_affected_sql(time_rollup)
        sql += ") \n"

    sql += "WHERE %s BETWEEN '%s' AND '%s' " % (
        time_rollup['time_source'], range_start, range_end)
    if not (table_config['timezone'].upper() == 'UTC'
            and time_rollup['timezone'].upper() == 'UTC'):
        sql += " AND p.utc_start =CAST('%s' AS TIMESTAMP) AND p.utc_end = " \
               "CAST('%s' AS TIMESTAMP)" % (
                   range_start, range_end)

    col_count = len(time_rollup['keys']) + len(rollup_keys)
    sql += "GROUP BY %s" % (",".join(str(x) for x in range(1, col_count + 1)))

    return sql


def _check_existing_rows_sql(rollup_config, rollup_name, time_name, rollup_date,
                             range_start, range_end):
    table_config = rollup_config['ROLLUP_CONFIG'][rollup_name]
    time_rollup = table_config['time_rollups'][time_name]

    sql = "SELECT 1 "
    if table_config['timezone'].upper() == 'UTC' \
            and time_rollup['timezone'].upper() == 'UTC':
        sql += " FROM %s f " % (time_rollup['table'])
    else:
        sql += "FROM %s f INNER JOIN tmp_affected_rollup p ON (" % (
            time_rollup['table'])
        sql += _join_affected_sql(time_rollup)
        sql += ") "

    sql += "WHERE %s=%s" % (time_rollup['time_destination'],
                            _map_time_param(rollup_date,
                                            time_rollup['time_destination']))
    if not (table_config['timezone'].upper() == 'UTC'
            and time_rollup['timezone'].upper() == 'UTC'):
        sql += "  AND p.utc_start =CAST('%s' AS TIMESTAMP) AND p.utc_end = " \
               "CAST('%s' AS TIMESTAMP)" % (
                   range_start, range_end)

    sql += ' LIMIT 1'

    return sql


def _delete_existing_rows_sql(rollup_config, rollup_name, time_name,
                              rollup_date, range_start, range_end):
    table_config = rollup_config['ROLLUP_CONFIG'][rollup_name]
    time_rollup = table_config['time_rollups'][time_name]

    sql = "DELETE FROM %s " % (time_rollup['table'])
    sql += "WHERE %s=%s " % (time_rollup['time_destination'],
                             _map_time_param(rollup_date,
                                             time_rollup['time_destination']))
    if not (table_config['timezone'].upper() == 'UTC'
            and time_rollup['timezone'].upper() == 'UTC'):
        sql += "AND %s" % (
            _delete_clause_affected_sql(time_rollup, range_start,
                                        range_end))

    return sql


def _delete_existing_transition_table_rows_sql(rollup_config, rollup_name,
                                               time_name, rollup_date,
                                               range_start, range_end):
    table_config = rollup_config['ROLLUP_CONFIG'][rollup_name]
    time_rollup = table_config['time_rollups'][time_name]

    sql = "DELETE FROM %s " % (time_rollup['transition_table'])
    sql += "WHERE %s=%s " % (time_rollup['time_destination'],
                             _map_time_param(rollup_date,
                                             time_rollup['time_destination']))
    if not (table_config['timezone'].upper() == 'UTC'
            and time_rollup['timezone'].upper() == 'UTC'):
        sql += "AND %s" % (
            _delete_clause_affected_sql(time_rollup, range_start,
                                        range_end))

    return sql


def _update_load_state(db, rollup_config, time_rollup, last_hour):
    # Update load_state only if last_hour is <= source data's last hour or if
    #  we don't have source data's last hour
    if 'source_load_state_variable_name' in time_rollup and \
                    'load_state_variable_name' in time_rollup:
        src_load_state_val=[row for row in db.cursor().execute(rollup_config['QUERY_LOAD_STATE'],tuple(time_rollup['source_load_state_variable_name']))]

        if src_load_state_val:
            LoadState(
                db, time_rollup['load_state_variable_name']
            ).upsert(last_hour)


# Returns a tuple of four values:
# row_count:   platforms affects
# rollup_date: reporting date in the table
# range_start: first source hour for this data
# range_end:   last source hour for this data
def load_affected_platforms(db, rollup_config, rollup_name, time_name,
                            last_hour,logger):
    table_config = rollup_config['ROLLUP_CONFIG'][rollup_name]
    time_rollup = table_config['time_rollups'][time_name]
    items = ['AFFECTED', table_config['timezone'].upper(),
             time_rollup['timezone'].upper(), time_rollup['interval'].upper()]
    if 'from_interval' in time_rollup:
        items.append(time_rollup['from_interval'].upper())
    platform_affected_sql_key = '_'.join(items)

    db.cursor().execute(rollup_config['AFFECTED_DROP_TMP'])
    db.cursor().execute(rollup_config['AFFECTED_CREATE_TMP'])

    bind_count = rollup_config[platform_affected_sql_key].count('?')

    platform_count =[row[0] for row in db.cursor().execute(rollup_config[platform_affected_sql_key],
                   tuple(repeat(last_hour, bind_count)))][0]
    # Gah.
    db.commit()

    # No platforms are affected
    if platform_count == 0:
        return platform_count, None

    # Reset the platform_affected_sql_key to one that does not require the
    # from_interval if exists.
    items = ['AFFECTED', table_config['timezone'].upper(),
             time_rollup['timezone'].upper(), time_rollup['interval'].upper(),
             'GET', 'DATES']
    platform_dates = '_'.join(items)
    bind_count = rollup_config[platform_dates].count('?')

    dates = \
        [row for row in db.cursor().execute(
            rollup_config[platform_dates],
            tuple(repeat(last_hour,
                         bind_count)))]
    logger.info(dates)
    if not len(dates):
        raise Exception("0 row returned when consolidating platform dates.")

    return platform_count, dates


def run_rollup_script(db, rollup_config, rollup_name, time_name, last_hour,
                      queue_job):
    table_config = rollup_config['ROLLUP_CONFIG'][rollup_name]
    time_rollup = table_config['time_rollups'][time_name]
    script_path = time_rollup['script']

    if time_rollup['interval'] == 'hour':
        interval_time = last_hour.strftime("%Y-%m-%d %H:%M:%S")
    else:
        raise Exception(
            "Unsupported time rollup %s" % (time_rollup['interval']))

    args = [script_path, '--start-datetime', interval_time]
    subprocess.call(args)

    # queue next rollup job if any
    if queue_job and 'queue_next_rollup' in time_rollup \
            and time_rollup['queue_next_rollup']:
        queue_hour(db, rollup_config, rollup_name,
                   time_rollup['queue_next_rollup'], last_hour)

    # update load_state
    _update_load_state(db, rollup_config, time_rollup, last_hour)

    db.commit()


# Run a rollup for a given period.
# last_hour is a UTC hour, and all rollup intervals that end on that last
# hour will
# be rolled up.
def run_rollup(db, rollup_config, rollup_name, time_name, last_hour, queue_job,logger):

    table_config = rollup_config['ROLLUP_CONFIG'][rollup_name]

    time_rollup = table_config['time_rollups'][time_name]

    if 'script' in time_rollup:
        return run_rollup_script(db, rollup_config, rollup_name, time_name,
                                 last_hour, queue_job)

        # Construct the list of platforms
    (platform_count, dates) = load_affected_platforms(db, rollup_config,
                                                      rollup_name, time_name,
                                                      last_hour,logger)

    # If we have no platforms to roll up, we can exit now.
    if platform_count == 0:
        if queue_job and 'queue_next_rollup' in time_rollup \
                and time_rollup['queue_next_rollup']:
            queue_hour(db, rollup_config, rollup_name,
                       time_rollup['queue_next_rollup'], last_hour)
        # update load_state
        _update_load_state(db, rollup_config, time_rollup, last_hour)
        db.commit()
        return

    for date in dates:
        (rollup_date, range_start, range_end) = (date[0], date[1], date[2])

        row_sql = _check_existing_rows_sql(rollup_config, rollup_name,
                                           time_name, rollup_date, range_start,
                                           range_end)

        if db.cursor().execute(row_sql).fetchone() is not None:
            del_sql = _delete_existing_rows_sql(rollup_config, rollup_name,
                                                time_name, rollup_date,
                                                range_start, range_end)
            db.cursor().execute(del_sql)

        if 'transition_table' in time_rollup:
            del_sql = _delete_existing_transition_table_rows_sql(rollup_config,
                                                                 rollup_name,
                                                                 time_name,
                                                                 rollup_date,
                                                                 range_start,
                                                                 range_end)
            db.cursor().execute(del_sql)
        logger.info(row_sql)
        rollup_sql = run_rollup_sql(db,rollup_config, rollup_name, time_name,
                                    rollup_date, range_start, range_end)
        logger.info(rollup_sql)

        db.cursor().execute(rollup_sql)

    if queue_job and 'queue_next_rollup' in time_rollup \
            and time_rollup['queue_next_rollup']:
        queue_hour(db, rollup_config, rollup_name,
                   time_rollup['queue_next_rollup'], last_hour)
    # update load_state
    _update_load_state(db, rollup_config, time_rollup, last_hour)
    db.commit()


def queue_hour(db, rollup_config, rollup_name, time_name, hour_changed):
    rollups = set()
    for rollup in build_rollup_jobs(db, rollup_config, rollup_name, time_name,
                                    hour_changed):
        rollups.add(rollup)
    # only queue new ones
    existing_jobs = RollupJob.load_all(db, rollup_config,rollup_name)
    rollups = rollups.difference(existing_jobs)
    RollupJob.save_all(db, rollup_config, rollups)


# For the hour changed, get list of last-hours in all relevant timezones that
#  need to rollup.
def build_rollup_jobs(db, rollup_config, rollup_name, time_name, hour_changed):
    table_config = rollup_config['ROLLUP_CONFIG'][rollup_name]
    time_rollup = table_config['time_rollups'][time_name]

    rollup_sql_key = "ROLLUP_%s_%s_%s" % (table_config['timezone'].upper(),
                                          time_rollup['timezone'].upper(),
                                          time_rollup['interval'].upper())

    sql_statement = rollup_config[rollup_sql_key]
    bind_count = sql_statement.count('?')

    # Check if the sql_statement is referencing the load_state table.
    if all(word in sql_statement for word in ['load_state', 'variable_name']):
        # If so, we need to also retrieve a load state variable.
        load_state_variable_name = time_rollup[
            'source_load_state_variable_name']

        # In order to prevent preemptive rollups, the queries now include the
        # load_state value corresponding
        # to the load state variable name.  This means that the hour changed
        # will appear one less than
        # the total number of times a '?' appears in the query.
        sql_arguments_tuple = tuple(chain(repeat(hour_changed, bind_count - 1),
                                          [load_state_variable_name]))
    else:
        # Otherwise the only argument - we asume - is the hour changed.
        sql_arguments_tuple = tuple(repeat(hour_changed, bind_count))

    for row in db.cursor().execute(sql_statement, sql_arguments_tuple):
        yield RollupJob(config=rollup_config, rollup_type=rollup_name,
                        rollup_time_name=time_name,
                        instance_rollup_date=row[0], last_hour=row[1])


# Run all of the defined rollups given a rollup configuration.
# last_hour is a UTC hour, and all rollup intervals that end on that last
# hour will
# be rolled up.
def run_rollups(db, rollup_config, rollup_name, last_hour,logger):
    table_config = rollup_config['ROLLUP_CONFIG'][rollup_name]

    for time_name, time_rollup in table_config['time_rollups'].items():
        # skip base
        if time_name != 'base':
            run_rollup(db, rollup_config, rollup_name, time_name, last_hour,
                       False,logger)


def queue_rollups(db, rollup_config, rollup_name, last_hour):
    table_config = rollup_config['ROLLUP_CONFIG'][rollup_name]

    rollup_config = {**CONFIG, **rollup_config}

    for time_name, time_rollup in table_config['time_rollups'].items():
        # skip base
        if time_name != 'base':
            queue_hour(db, rollup_config, rollup_name, time_name, last_hour)

# Return a list of all the tables in the rollup configuration
def valid_rollup_tables(rollup_config):
    return filter(lambda x: 'time_rollups' in rollup_config[x],
                  rollup_config)


def valid_rollup_times(rollup_config, rollup_table):
    return rollup_config['ROLLUP_CONFIG'][rollup_table]['time_rollups'].keys()
