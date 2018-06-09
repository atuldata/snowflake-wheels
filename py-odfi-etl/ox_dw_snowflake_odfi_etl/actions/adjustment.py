"""
Upload/ELT a particular job. Job will use load_state and odfi_etl_status
 tables to determine what to run for.
"""
from ._base import acquire_lock, _loader
from .actors.adjustmenter import Adjustmenter

OPTIONS = ["job_name", "debug"]


def adjustment(options, dbh):
    """
    Run etl on a particular job.
    :param options: NamedTuple including options defined for this action in
                    .actions.py
    """
    name = '_'.join([options.job_name, 'adjustment'])
    acquire_lock(name)
    # Loop until no more.
    while _loader(name, options.job_name, Adjustmenter, options, dbh):
        pass
