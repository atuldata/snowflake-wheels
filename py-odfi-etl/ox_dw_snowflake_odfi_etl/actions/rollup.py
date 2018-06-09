"""
Download available partfiles for a given job name.
"""
# Example use case:
#
# To queue rollups because something changed in source tables between these dates (dates are inclusive):
#
# odfi_etl rollup -j ox_transaction_sum_and_domain_hourly --rollup_start-date "2013-03-15 00:00:00" --rollup_end-date "2013-03-16 00:00:00" \
#             --rollup_name ROLLUP_ox_transaction_sum  --rollup_interval_type day
#
#To simply run a rollup from the queue
# odfi_etl rollup -j ox_transaction_sum_and_domain_hourly
#
# To run a rollup for a start date
# odfi_etl rollup -j ox_transaction_sum_and_domain_hourly --rollup_start-date "2013-03-15 00:00:00"
#
# To run a rollup till the end date
# odfi_etl rollup -j ox_transaction_sum_and_domain_hourly --rollup_start-date "2013-03-15 00:00:00" --rollup_end-date "2013-03-16 00:00:00"
#
# To preview the queue for the rollup without running, it will log the information
# odfi_etl rollup -j ox_transaction_sum_and_domain_hourly --rollup_start-date "2013-03-15 00:00:00" --rollup_end-date "2013-03-16 00:00:00" --preview_rollup_queue
#
# The above commands will will calculate and queue all the affected hours the hour(s) changed
# such the day containing that hour in all platform will get re-rolled.
# Depending whether queue_next_rollup is set, it will queue the next rollup accordingly.
#
# NOTE that a queued job (espcially monthly rollup jobs) can get run before the base data (i.e. daily)
# is available. When that happens, that job will do nothing and removed from its queue. When the base data
# is available, either the same rollup job will get queued again or that base job call invoke the rollup
# job directly.  As for load_state, it will only be updated if the base data is more current.
#
# To actually run all queued rollups:
#
# odfi_etl rollup -j ox_transaction_sum_and_domain_hourly

from ._base import acquire_lock, _loader
from .actors.rolluper import Rolluper

OPTIONS = ["job_name", "debug", "rollup_name","rollup_start_date","rollup_end_date","rollup_interval_type","preview_rollup_queue","run_rollup_queue"]

def rollup(options, dbh):
    """
    Download available partfiles and meta data for a given job_name.
    """
    name = '_'.join([options.job_name, 'rolluper'])
    acquire_lock(name)
    # Loop until no more.
    while _loader(name, options.job_name, Rolluper, options, dbh):
        pass

