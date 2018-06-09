"""
Non-specific utils here. To be shared for all.
"""
import sys
import warnings


def ignore_warnings(my_func):
    """
    This is a decorator used to suppress warnings.
    """

    def wrapper(self, *args, **kwargs):
        """
        This is where the warning suppression occurs.
        """
        if sys.version_info >= (3, 2):
            warnings.simplefilter("ignore", ResourceWarning)
        with warnings.catch_warnings():
            my_func(self, *args, **kwargs)

    return wrapper
