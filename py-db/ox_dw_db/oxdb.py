"""
Base for db connections for OX.
"""
import pyodbc
import psycopg2
import snowflake.connector
from .settings import ENV


class OXDB(object):
    """
    This will return a database connection.
    Supports connecting to pyodbc supporting databases, postgres and snowflake.
    Required:
        1) connection_name: A defined DB_CONNECTION in env.yaml
    Optional:.
        1) transactions_support - default is True. For non-transactional
               data sources like Impala, Hive it should be set to False.
    Examples:
        db = OXDB(connection_name)
        # Iterate over returned rows
        for row in db.get_executed_cursor(query):
            ...
        with OXDB(connection_name) as oxdb:
            for statement in statements:
                oxdb.execute(statement)
            # You can commit as you go along
            oxdb.commit()
            # Or will commit on exit if not errors.
            # Will rollback if an exception is raised within the with block.
    """

    def __init__(self, connection_name, transactions_support=True):
        self.connection_name = connection_name
        self.transaction_support = transactions_support
        self._connection = None

    def close(self):
        """
        Closes the connection.
        """
        self.connection.close()

    def commit(self):
        """
        Calls commit on the connection.
        """
        self.connection.commit()

    @property
    def connection(self):
        """
        Connection is created once for the instance.
        For transactional databases autocommit is set to False and
        for non transactional databases autocommit is set to True.
        """
        if self._connection is None:
            conf = ENV['DB_CONNECTIONS'].get(self.connection_name)
            if conf is None:
                raise ValueError("Invalid connection name %s!" % self.connection_name)

            if conf['TYPE'] == 'ODBC':
                """
                Databases with ODBC connection type use pyodbc to establish connection.
                """
                if self.transaction_support:
                    self._connection = pyodbc.connect(
                        "DSN=%s" % conf['DSN'], autocommit=False)
                else:
                    """
                    For non transaction databases like Impala autocommit should be explicitly
                    specified to prevent ODBC error. After connection is made pyodbc
                    attempts to turn the autocommit feature off by default, hence
                    pyodbc throws error for non transactional database if autocommit
                    is not specified.
                    """
                    self._connection = pyodbc.connect(
                        "DSN=%s" % conf['DSN'], autocommit=True)
            elif conf['TYPE'] == 'POSTGRESQL':
                """
                For postgres database use psycopg2 for connection.
                """
                self._connection = psycopg2.connect(**conf['KWARGS'])
            elif conf['TYPE'] == 'SNOWFLAKE':
                """
                For snowflake database using python snow connect for database connection.
                """
                self._connection = snowflake.connector.connect(**conf['KWARGS'])
            else:
                raise ValueError("Not configured for type %s!" % conf['TYPE'])

        return self._connection

    def execute(self, *args, **kwargs):
        """
        Executes a statement and returns the row count affected.
        """
        cursor = self.get_executed_cursor(*args)
        if 'commit' in kwargs and isinstance(kwargs['commit'], bool) \
                and kwargs['commit']:
            self.commit()

        return cursor.rowcount

    def get_executed_cursor(self, *args):
        """
        Returns a cursor with the executed statement.
        """
        cursor = self.connection.cursor()
        cursor.execute(*args)

        return cursor

    def rollback(self):
        """
        Calls rollback() on the connection.
        """
        self.connection.rollback()

    def __enter__(self):
        """
        Allows to be used in a with block.
        """
        return self

    def __exit__(self, exception_type, exception_value, traceback):
        """
        Allows to be used in a with block.
        """
        if exception_type is not None:
            self.rollback()
        else:
            self.commit()

        self.close()

    def __del__(self):
        """
        Python garbage collector method called at the end of object life cycle.
        Used to close the connection.
        """
        try:
            self.close()
        except Exception:
            pass
