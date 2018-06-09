"""
Exception Classes here.
"""


class BreakCheckError(Exception):
    """
    Case when a break_check sql returns rows.
    """
    pass


class JobNotFoundException(Exception):
    """
    Case when there is no record in the load_state table.
    """

    def __init__(self, job_name):
        super(JobNotFoundException,
              self).__init__("Job(%s) not found! Please add." % job_name)
