"""
Use for managing the load state.
"""
from datetime import datetime
from dateutil.parser import parse as parse_date

CREATE = """
CREATE TABLE load_state (
    variable_name varchar(100) NOT NULL,
    variable_value varchar(100) NOT NULL,
    created_datetime timestamp NOT NULL,
    modified_datetime timestamp NOT NULL
)"""

DELETE = """
DELETE FROM load_state WHERE variable_name = '%(variable_name)s'"""

EXISTS = """
SELECT 1 FROM load_state WHERE variable_name = '%(variable_name)s'"""

INSERT = """
INSERT INTO
load_state(
    variable_value, variable_name, created_datetime, modified_datetime)
VALUES('%(variable_value)s', '%(variable_name)s',
       '%(created_datetime)s', '%(modified_datetime)s')"""

SELECT = """
SELECT variable_value, created_datetime, modified_datetime
FROM load_state
where variable_name = '%(variable_name)s'"""

UPDATE = """
UPDATE load_state
SET variable_value = '%(variable_value)s',
    modified_datetime = '%(modified_datetime)s'
WHERE variable_name = '%(variable_name)s'"""

NEXTVAL = """
SELECT %(seq_name)s.nextval"""

CUSTOM_DATE_STRINGS = ['%Y-%m-%d_%H %Z']


def parse_date_string(date_string):
    """
    This is used mainly for our own date formats such as 2017-02-02_10.
    Most other formats will be parsed just fine with dateutil.parser.
    """

    def _parse_date_string(_date_string, _date_format):
        try:
            return datetime.strptime(_date_string, _date_format)
        except ValueError:
            return None

    for date_format in CUSTOM_DATE_STRINGS:
        date_time = _parse_date_string(date_string, date_format)
        if date_time is not None:
            return date_time

    return parse_date(date_string)


class LoadState(object):
    """
    For working with the load state.
    Requires valid dbh object.
    variable_name can be set after instantiation.
    """
    _exists = None

    def __init__(self, dbh, variable_name=None):
        self.dbh = dbh
        self.variable_name = variable_name
        self.variable_value = None
        self.created_datetime = None
        self.modified_datetime = None
        try:
            cursor = self.dbh.cursor()
            cursor.execute("SELECT 1 FROM load_state")
        except Exception as exc:
            # Broad exception here as it will be connection specific.
            if 't exist' in str(exc) or 'no such table' in str(exc):
                cursor.execute(CREATE)
            else:
                raise
        self.select()

    def delete(self):
        """
        Remove the varible_name from the load_state table.
        """
        cursor = self.dbh.cursor()
        cursor.execute(DELETE % {'variable_name': self.variable_name})
        self.dbh.commit()
        self._exists = None

    @property
    def exists(self):
        """
        Does the variable_name already exist in the load_state table?
        """
        if self._exists is None:
            cursor = self.dbh.cursor()
            cursor.execute(EXISTS % {'variable_name': self.variable_name})
            self._exists = bool(cursor.fetchone())

        return self._exists

    def select(self):
        """
        Sets local values to the db values.
        """
        cursor = self.dbh.cursor()
        cursor.execute(SELECT % {
            'variable_name': self.variable_name
        })
        row = cursor.fetchone()
        if row is not None:
            (self.variable_value, self.created_datetime,
             self.modified_datetime) = [item for item in row]
            self._exists = True

    def increment_by_seq(self, seq_name, commit=False):
        self.upsert(
            self.dbh.cursor().execute(
                NEXTVAL % {'seq_name': seq_name}
            ).fetchone()[0])

    def update_variable_datetime(self,
                                 variable_value=None,
                                 commit=False,
                                 force=False):
        """
        Will only update the variable_value to a string representation of a
        datetime or parsable string passed in as variable_value.
        Variable value must be a datetime.datetime object.
        """
        if variable_value is not None:
            new_value = variable_value
            if isinstance(new_value, str):
                new_value = parse_date_string(new_value)
            if new_value is not None:
                if not force and self.variable_value is not None:
                    current_value = parse_date_string(self.variable_value)
                    if current_value is not None and new_value < current_value:
                        if commit:
                            self.dbh.commit()
                        return
                self.upsert(
                    new_value.strftime('%Y-%m-%d %H:%M:%S'), commit=commit)

    def update_variable_intrvl(self, variable_value, commit=False,
                               force=False):
        """
        Will format the typical intrvl value '%Y-%m-%d_%H %Z' then update as
        datetime.
        """
        self.update_variable_datetime(
            datetime.strptime('%s UTC' % variable_value,
                              '%Y-%m-%d_%H %Z').strftime('%Y-%m-%d %H:00:00'),
            commit=commit,
            force=force)

    def upsert(self,
               variable_value=datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S'),
               commit=False):
        """
        Update the load_state. Insert it if it does not exist.
        """
        statement = UPDATE if self.exists else INSERT
        statement %= {
            'variable_value': variable_value,
            'variable_name': self.variable_name,
            'created_datetime':
            datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S'),
            'modified_datetime':
            datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S')
        }
        cursor = self.dbh.cursor()
        cursor.execute(statement)
        if commit:
            self.dbh.commit()
        self.select()

    insert = upsert

    update = upsert
