"""
User defined exception class.
"""


class UserException(Exception):
    """
    User defined exception to raise specific error scenarios.
    """
    def __init__(self, err_message):
        self.message = err_message
