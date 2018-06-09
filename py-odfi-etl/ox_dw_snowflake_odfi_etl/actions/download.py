"""
Download available partfiles for a given job name.
"""
from ._base import acquire_lock, _loader
from .actors.downloader import Downloader

OPTIONS = ["job_name", "debug"]


def download(options, dbh):
    """
    Download available partfiles and meta data for a given job_name.
    """
    name = '_'.join([options.job_name, 'downloader'])
    acquire_lock(name)
    # Loop until no more.
    while _loader(name, options.job_name, Downloader, options, dbh):
        pass
