
from datetime import datetime,  timedelta
from ._loader_base import _LoaderBase
from ...common.rollup_util import RollupJob, run_rollups, queue_rollups
from ...common.settings import APP_CONF_ROOT, get_conf

CONFIG = get_conf('rollup', APP_CONF_ROOT)

class Rolluper(_LoaderBase):
    def __init__(self, job):
        super(Rolluper, self).__init__(job)

        self.rollup_config = {**CONFIG, **self.job.config}
        self.rollup_start_date=self.job.rollup_start_date
        self.rollup_end_date = self.job.rollup_end_date

        if not self.job.rollup_name:
            self.rollup_name_list = [rollup_name for rollup_name in self.job.config['ROLLUP_CONFIG'] if 'ROLLUP_' in rollup_name]

        else:
            self.rollup_name_list = [self.job.rollup_name for rollup_name in self.job.config['ROLLUP_CONFIG'] if self.job.rollup_name in rollup_name]

        if self.rollup_start_date or self.rollup_end_date:
            if not self.rollup_end_date:
                self.rollup_end_date = self.rollup_start_date

            self.rollup_start_date = datetime.strptime(self.rollup_start_date, '%Y-%m-%d %H:%M:%S')
            self.rollup_end_date = datetime.strptime(self.rollup_end_date, '%Y-%m-%d %H:%M:%S')

            if self.rollup_start_date > self.rollup_end_date:
                self.job.logger.error("Start date is after end date")

        if len(self.rollup_name_list)==0:
            self.job.logger.info("There is no rollup config: %s" % (", ".join(self.rollup_name_list)))
            return

    def run_jobs(self, rollup_name):
        done = False
        while not done:
            current_job = RollupJob.process_one(self.job.dbh, self.rollup_config, rollup_name, self.job.logger)
            if not current_job:
                done = True

    def __call__(self):
        if not self.job.preview_rollup_queue and not self.job.rollup_start_date \
                and not self.job.rollup_end_date and not self.job.rollup_interval_type and not self.job.run_rollup_queue:
            for rollup_name in self.rollup_name_list:
                self.run_jobs(rollup_name)

        elif self.job.run_rollup_queue:
            if (self.job.rollup_start_date):
                run_rollups(self.job.dbh, self.rollup_config, self.job.rollup_name, self.job.rollup_start_date)
            else:
                if self.job.preview_rollup_queue:
                    for rollup_name in self.rollup_name_list:
                        for job in RollupJob.load_all(self.job.dbh, self.rollup_config,rollup_name):
                            self.job.logger.info(job)
                else:
                    for rollup_name in self.rollup_name_list:
                        self.run_jobs(rollup_name)
        else:
            if self.job.rollup_start_date:
                now = self.rollup_start_date
                while now <= self.rollup_end_date:
                    for rollup_name in self.rollup_name_list:
                        queue_rollups(self.job.dbh, self.rollup_config, rollup_name, now)
                    now = now + timedelta(hours=1)
        return False