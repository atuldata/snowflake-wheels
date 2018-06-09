#!/opt/bin/python -u

import sys
import os
import yaml
import time
from ox_dw_load_state import LoadState
from ._loader_base import _LoaderBase

from ...common.settings import APP_CONF_ROOT, get_conf


# Job_queue_lib is a framework for queuing up and processing jobs after the ETL is run.
# Example jobs that could be queued: variable dealtype revenue adjustments, rollups etc.
# A detailed description of the framework behaviour can be found in https://wiki.corp.openx.com/pages/viewpage.action?spaceKey=~rashmi.naik&title=Generic+Adjustments+framework#?lucidIFH-viewer-6aa01c1c=1
#
# Use Cases:
# 1. A base hourly job can call queue_job(...) to do revenue adjustments after loading the data for the given hour. The process_queue job will call the
#    job_queue_lib with the job name and key that needs to processesed.  See ox_video_traffic_hourly.yaml for the configurations that will be used by job_queue_lib framework.
#    Detailed description about yaml file can be found in: https://wiki.corp.openx.com/display/~rashmi.naik/Sample+yaml+file+for+job+queue+lib+framework
#    The job_queue_lib can be explicitly called as: python job_queue_lib.py "/var/feeds/python/ox_video_traffic_hourly.py" "REVENUE_ADJUSTMENT"
# 2. If we want to run an adjustment manually, just add an entry to mstr_datamart.queue_state table and call the job_queue_lib framework as: python job_queue_lib.py <job_path/job_name.py> <KEY>
#    The configuration file metioned in the queue_state table will be used for job processing.


#Framework variables:
PRECONDITION = "PRECONDITION"
PRECONDITION_FALIED = "PRECONDITION_FALIED"
MODULES = "MODULES"
ACTIONS = "ACTIONS"
ACTION = "ACTION"
TASKS = "TASKS"
VARIABLE_ASSIGNMENT = "VARIABLE_ASSIGNMENT"
VAR_NAMES = "VAR_NAMES"
VAR_SRC = "VAR_SRC"
FUNCTION = "FUNCTION"
FUNC_NAME = "FUNC_NAME"
FUNC_ARGS = "FUNC_ARGS"
SQLS = "SQLS"
SQL = "SQL"
SQL_ARGS = "SQL_ARGS"
UPDATE_LOAD_STATE = "UPDATE_LOAD_STATE"

class Adjustmenter(_LoaderBase):
    def __init__(self,job):
        super(Adjustmenter, self).__init__(job)
        self.config = get_conf('adjustment',APP_CONF_ROOT)
        self.revenue_job= dict()  # this map will have all the varaibles related to the job that is executing currently
        self.revenue_job["jq_job_name"] = self.job.name
        self.revenue_job["jq_key"] = 'REVENUE_ADJUSTMENT'


    def __call__(self):
        self.job.logger.info("Getting queued entry for the job: "+self.revenue_job["jq_job_name"] + " key: " +self.revenue_job["jq_key"] )
        job_details = self.job.dbh.get_executed_cursor(self.config['GET_QUEUED_JOB'],self.revenue_job["jq_job_name"],self.revenue_job["jq_key"] )
        try:
            for each_job in job_details:
                #get the job details for the hour we are processing
                self.initialize_job(each_job)
                self.job.logger.info("Processing job with jq_instance_sid %s", self.revenue_job["jq_instance_sid"])

                #Check precondition
                execute_job=self.check_precondition(self.revenue_job["jq_config_file"][self.revenue_job["jq_key"]][PRECONDITION])

                #Execute prcondition failed action if any precon fails and failed action exists!
                if not execute_job:
                    self.job.logger.info("Precondition failed!")
                    if PRECONDITION_FALIED in self.revenue_job["jq_config_file"][self.revenue_job["jq_key"] ]:
                        self.job.logger.info("Executing precondition failed action...")
                        self.precondition_failed()
                        self.job.dbh.commit()
                    break

                # change jq_state to running
                self.change_state('RUNNING')
                self.job.logger.info("Chaning  state to RUNNING for jq_instance_sid %s", self.revenue_job["jq_instance_sid"])
                self.job.dbh.commit()

                #Dynamically import the required modules
                if MODULES in self.revenue_job["jq_config_file"][self.revenue_job["jq_key"] ]:
                    self.job.logger.info("Importing modules...")
                    self.import_modules(self.revenue_job["jq_config_file"][self.revenue_job["jq_key"] ][MODULES])

                #Execute actions
                self.job.logger.info("Starting to execute actions...")
                self.execute_actions()
                self.change_state('SUCCESS')
                self.job.logger.info("Changed state to SUCCESS for jq_instance_sid %s", self.revenue_job["jq_instance_sid"])
                self.job.dbh.commit()
                self.revenue_job["jq_job_initialized"]  = False

        except:
            self.job.logger.error("Exception %s",sys.exc_info()[0])
            if "jq_job_initialized" in self.job and self.revenue_job["jq_job_initialized"] :
                self.job.dbh.rollback()
                self.change_state('FAILED')
                self.job.logger.info("Changed state to FAILED for jq_instance_sid %s", self.revenue_job["jq_instance_sid"])
                self.change_state('QUEUED')
                self.job.logger.info("Changed state to QUEUED for jq_instance_sid %s", self.revenue_job["jq_instance_sid"])
                self.job.dbh.commit()
            raise

    def initialize_job(self, each_job):
        self.revenue_job["jq_instance_sid"] = each_job[0]
        self.revenue_job["jq_config_file_name"] = each_job[1]
        self.revenue_job["jq_config_file"] = yaml.load(open(self.revenue_job["jq_config_file_name"] ))
        self.revenue_job["jq_utc_date_sid"] = each_job[2]
        self.revenue_job["jq_utc_timestamp"] = each_job[3]
        self.revenue_job["jq_state"] = each_job[4]
        self.revenue_job["jq_host"] = each_job[5]
        self.revenue_job["jq_is_republish"] = each_job[6]
        self.revenue_job["jq_rollup_config"] = yaml.load(open(self.revenue_job["jq_config_file"]['ROLLUP_CONFIG'])) #needs to be changed when we move rollup under this framework
        self.revenue_job["jq_pid"] = os.getpid()
        self.revenue_job["jq_db"] = self.job.dbh
        self.revenue_job["jq_job_initialized"] = True

    def import_modules(self, module_names):
        for each_module in module_names:
            m = __import__(each_module)
            self.revenue_job["jq_module_"+each_module] = m


    def execute_sqls(self, sql_list):
        for sqls in sql_list:
            self.job.logger.info("Executing SQL: %s",sqls[SQL])
            if SQL_ARGS in sqls:
                args = [] # create the list of args that is required to execute this sql
                for each_var in sqls[SQL_ARGS]:
                    args.append(self.revenue_job[each_var])
                self.job.dbh.execute(sqls[SQL],args)
            else:
                self.job.dbh.execute(sqls[SQL])

    def precondition_failed(self):
        post_sql_list = self.revenue_job["jq_config_file"][self.revenue_job["jq_key"] ][PRECONDITION_FALIED]
        self.execute_sqls(post_sql_list)

    def check_precondition(self, precondition):
        for sqls in precondition:
            if SQL_ARGS in sqls:
                args = [] # create the list of args that is required to execute this sql
                for each_var in sqls[SQL_ARGS]:
                    args.append(self.revenue_job[each_var])

                status = self.job.dbh.get_executed_cursor(sqls[SQL],args).fetchall()[0][0]
            else:
                status = self.job.dbh.get_executed_cursor(sqls[SQL]).fetchall()[0][0]

            self.job.logger.info("Precondition %s return value:%s",sqls[SQL],status)

            #returning anything other than true will be treated as failed precon
            if not status: #if any pre condition fails return false without further check
                return status

        return status

    def change_state(self, jq_state):
        if jq_state=='QUEUED':
            self.revenue_job["jq_instance_sid"] = self.job.dbh.get_executed_cursor(self.config['NEXT_INSTANCE_SID']).fetchall()[0][0]
        self.job.dbh.execute(self.config['CHANGE_STATE'], (self.revenue_job["jq_instance_sid"]),(self.revenue_job["jq_job_name"]),(self.revenue_job["jq_config_file_name"] ),(self.revenue_job["jq_key"] ),(self.revenue_job["jq_utc_date_sid"] ),(self.revenue_job["jq_utc_timestamp"] ),jq_state,(self.revenue_job["jq_host"] ),(self.revenue_job["jq_pid"] ),(self.revenue_job["jq_is_republish"] ))


    def assign_variables(self, var_src, var_names):
        self.job.logger.info("Assigning variables:"+ ', '.join(var_names))
        cursor = self.job.dbh.get_executed_cursor(var_src)
        i = 0
        for row in cursor:
            for each_val in row: # assign values to the variables
                self.revenue_job[var_names[i]] = each_val
                i += 1

    def call_functions(self,class_name, func, params):
        self.job.logger.info("Calling function %s from %s", func,class_name)
        obj = self.revenue_job["jq_module_"+class_name] # get the module that is imported in the begining
        func = getattr(obj, func)
        if params is not None:
            args = [] # create the list of args that is required by func
            for each_var in params:
                args.append(self.revenue_job[each_var])

            func(*args) # call the function with parameters
        else:
            func()

    def update_load_state(self,var_name):
        LoadState(
            self.job.dbh, variable_name=var_name
        ).update_variable_datetime(variable_value=self.revenue_job["jq_utc_timestamp"])

    def execute_actions(self):
        actions = self.revenue_job["jq_config_file"][self.revenue_job["jq_key"] ][ACTIONS]
        for each_action in actions:
            action = each_action[ACTION]
            if PRECONDITION in action:
                execute_action = self.check_precondition(action[PRECONDITION])
                if not execute_action:
                    self.job.logger.info("Precondition failed, hence skipping this action...")
                    continue

            # optional precondition
            self.job.logger.info("Executing task...")
            tasks = action[TASKS]
            for each_task in tasks:
                #determine the task and do the appropriate thing
                if FUNCTION in each_task:
                    func = each_task[FUNCTION][FUNC_NAME] # expecting classname.funcname
                    class_name = func.split(".")[0]
                    fun_name = func.split(".")[1]
                    if FUNC_ARGS in each_task[FUNCTION]:
                        params = each_task[FUNCTION][FUNC_ARGS]
                    else:
                        params = None
                    self.call_functions(class_name,fun_name, params)

                elif VARIABLE_ASSIGNMENT in each_task:
                    var = each_task[VARIABLE_ASSIGNMENT]
                    self.assign_variables(var[VAR_SRC],var[VAR_NAMES])

                elif SQLS in each_task:
                    self.execute_sqls(each_task[SQLS])

                elif UPDATE_LOAD_STATE in each_task:
                    self.update_load_state(each_task[UPDATE_LOAD_STATE])

                else:
                    self.job.logger.error("Unknown TASK %s", each_task)
                    raise Exception("Unknown TASK found in action!")
        self.job.logger.info("Done executing all actions!")

    def queue_rev_adjustment(self, utc_date_sid, readable_interval, is_republish):

        self.revenue_job["jq_config_file_name"] = get_conf(self.job.name + "_revenue")
        self.revenue_job["jq_utc_date_sid"] = utc_date_sid
        self.revenue_job["jq_utc_timestamp"] = readable_interval
        self.revenue_job["jq_host"] = 'Uploader'
        self.revenue_job["jq_pid"] = os.getpid()
        self.revenue_job["jq_is_republish"] = is_republish
        self.change_state('QUEUED')
        self.job.dbh.commit()





