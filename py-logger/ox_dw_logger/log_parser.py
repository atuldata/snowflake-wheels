"""
For parsing etl logs.
"""
from collections import namedtuple
from datetime import datetime, timedelta
from glob import glob
import os
import re
import smtplib
import sqlite3
import sys
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
import msgpack
from prettytable import PrettyTable
from dateutil.parser import parse as date_parse
from .settings import (
    LOG_DIRECTORY, LOCAL_OUTPUT,
    LOG_DATE_REGEX, ENV, HOSTNAME
)

LogRow = namedtuple('LogRow',
                    ['log_file', 'transaction_time', 'tag', 'message'])
MAX_HOURS = 168
NAME = 'etl_log_parser'
NO_NEW_DATA = 'No new data'
TAGS = ['ERROR', 'CRITICAL']
TABLE_WRAPPER = """
<h3 style="font-family: monospace; color: red;">%s</h3>
%s
"""
TEXT_WRAPPER = """
\n%s
%s
"""


def email_report(
        tag,
        email=None,
        log_files=None,
        path=LOG_DIRECTORY):
    """
    Given list of log files and a single tag will email any new messages.
    """
    if email is None:
        email = ENV['CHECK_ERROR_EMAIL_RECIPIENTS']
    text, html_body = get_reports(tag, log_files=log_files, path=path)
    if text.startswith(NO_NEW_DATA):
        sys.stderr.write("%s\n" % text)
        return
    msg = MIMEMultipart('alternative')
    msg['Subject'] = "%ss logged from %s(%s)" % (tag, ENV['CLUSTER'], HOSTNAME)
    msg['From'] = ENV['CHECK_ERROR_EMAIL_SENDER']
    msg['To'] = email
    msg.attach(MIMEText(text, 'plain'))
    msg.attach(MIMEText(html_body, 'html'))
    smtp = smtplib.SMTP('127.0.0.1')
    smtp.sendmail(msg['From'], msg['To'], msg.as_string())
    smtp.quit()


def get_logs(path=LOG_DIRECTORY):
    """
    :param path:
    :return: Yield the log files found.
    """
    for sub_path in os.walk(path):
        for log_file in glob(os.path.join(sub_path[0], '*.log')):
            yield log_file


def get_reports(tag, log_files=None, since=None, path=LOG_DIRECTORY):
    """
    This will return both the html and the text bodies for your report.
    """
    text = ''
    html = ''
    for log_file, table in get_tables(tag, log_files or get_logs(path), since):
        if table.rowcount == 0:
            continue
        text += TEXT_WRAPPER % (log_file, table.get_string())
        html += \
            TABLE_WRAPPER % (
                log_file,
                table.get_html_string(
                    attributes={'name': NAME, 'border': 1, 'valign': 'top'}))
    if len(text) == 0:
        text = "%s since %s." % (NO_NEW_DATA, since)
        html = text
    else:
        # Gmail doesn't recognize the <style> tag or external css source
        # So here we are applying inline style.
        html = re.sub(r'>\n?\s+<', '><', html)
        html = \
            re.sub(
                '<tr',
                '<tr style="background: #FFF; font-family: monospace; color:'
                ' black;"',
                html)
        one = 'background: #FFF'
        two = 'background: #CCC'
        html = \
            re.sub(
                r'(%(one)s(?:(?!%(one)s).)*)%(one)s((?:(?!%(one)s).)*)' % {
                    'one': one},
                r'\1%s\2' % two, html)
        html = \
            re.sub(r'td style="', 'td style="font-family: monospace; ', html)
        html = re.sub('><', '>\n<', html)

    return text, html


def get_table(tag, log_file, since=None):
    """
    Returns all of the rows for a table for a single tag and log_file.
    """
    table = PrettyTable()
    table.field_names = ['transaction_time', 'tag', 'message']
    table.align = 'l'
    table.header = True
    table.format = True
    parser = LogParser(log_file, tag=tag, since=since)
    parser.store_new()
    for row in parser:
        table.add_row([row.transaction_time, row.tag, row.message])

    return table


def get_tables(tag, log_files=None, since=None):
    """
    yields table objects for later printing.
    Will attempt to use skip_bytes stored for each log.
    :param tag: Which tag do you want to report on? See log levels.
    :param log_files: List of log files.
    :param since: Time stamp of how far to get rows for.
    """
    for log_file in log_files:
        yield log_file, get_table(tag, log_file, since=since)


def print_report(tag, log_files=None, hours=1, html=False, path=LOG_DIRECTORY):
    """
    Given list of log files and a single tag will output to the console the
    messages.
    :param tag: Which tag do you want to report on? See log levels.
    :param log_files: List of log files.
    :param hours: How many hours back to report on.
    :param html: Print out html?
    :param path: If log_files is None then where to look for log files?
    """
    since = (
        datetime.now() - timedelta(hours=hours)
    ).strftime('%Y-%m-%d %H:%M:%S')
    text, html_body = \
        get_reports(tag, log_files=log_files, since=since, path=path)
    if html:
        print(html_body)
    else:
        print(text)


class LogParser(object):
    """
    Used for parsing out log lines of a certain tag back to a certain range.
    Manages local db cache.
    """

    def __init__(
            self,
            log_file,
            tag='ERROR',
            name=NAME,
            since=None,
            log_date_regex=LOG_DATE_REGEX,
            head_regex=None,
            local_db=None):
        self._log_file = log_file
        self._tag = tag
        self._name = name
        self._since = since
        self._log_date_regex = log_date_regex
        self._head_regex = \
            head_regex if head_regex is not None else \
            r'%s (%s): (.*)' % (self.log_date_regex, self.tag)
        self._local_db = local_db
        self._log_time = None
        self.local_db_file = \
            os.path.join(LOCAL_OUTPUT, '.'.join([self.name, 'db']))
        self._init_local_db()

    def __iter__(self):
        for row in self._rows():
            yield row

    def __next__(self):
        return next(self._rows())

    @property
    def head_regex(self):
        """
        This will match the start of a log line with the tag.
        Needed since much of the messages output are on multiple lines.
        """
        return self._head_regex

    @property
    def local_db(self):
        """
        Place to write locally the progress of the parsing.
        :return:
        """
        if self._local_db is None:
            self._local_db = sqlite3.connect(self.local_db_file)
            if sys.version_info < (3, 0):
                self._local_db.text_factory = str

        return self._local_db

    @property
    def log_date_regex(self):
        """
        This will tell us that the line is the start of a message. The default
        one use a formatted date. Your may be different but the parser needs
        to know how to identify the start of a line as many messages can be on
        multiple lines.
        By default this log parser works with the etl_logger so no need to
        override if that is what you are using.
        """
        return self._log_date_regex

    @property
    def log_file(self):
        """
        This is the log file that we are going to parse.
        """
        return self._log_file

    @property
    def log_time(self):
        """
        This is the create time for the log file.
        Needed since log files are often replaced during log rotation.
        """
        if self._log_time is None:
            try:
                # This works on OSX
                self._log_time = \
                    datetime.fromtimestamp(os.stat(self.log_file).st_birthtime)
                return self._log_time
            except AttributeError:
                # On Linux you cannot get the file createtime so we will look
                # the first line of the log_file.
                with open(self.log_file, 'r') as log_file:
                    for line in log_file:
                        matcher = re.match(self.log_date_regex, line)
                        if matcher:
                            self._log_time = date_parse(matcher.group(1))
                            return self._log_time

            self._log_time = datetime.now()

        return self._log_time

    @property
    def name(self):
        """
        This is the name we will use for this parser.
        Used for the table name in the local db.
        This way we can have others.
        """
        return self._name

    @property
    def since(self):
        """
        This should be a datetime object of how far back to read the logs.
        """
        return self._since

    @property
    def tag(self):
        """
        This parser will only return one log level per pass of the parser.
        Defaults to ERROR.
        """
        return self._tag

    def cleanup(self, max_hours=MAX_HOURS):
        """
        Remove log data from the local_db older than the MAX_HOURS.
        """
        transaction_time = self.since
        if transaction_time is None:
            transaction_time = (
                datetime.now() - timedelta(hours=max_hours)
            ).strftime('%Y-%m-%d %H:%M:%S')
        self.local_db.execute("""
            DELETE FROM %s_logs WHERE transaction_time < ?
        """ % self.name, [transaction_time])
        self.local_db.execute('VACUUM')
        self.local_db.commit()

    def get_since(self, max_hours=MAX_HOURS):
        """
        Will query the email db for the last transaction reported.
        """
        query = """
            SELECT max(transaction_time)
              FROM %s_email
             WHERE log_file = ? AND log_time = ? AND tag = ?""" % self.name
        for row in self.local_db.execute(
                query, [self.log_file, self.log_time, self.tag]):
            return row[0] or (
                datetime.now() - timedelta(hours=max_hours)
            ).strftime('%Y-%m-%d %H:%M:%S')

    def get_skip_bytes(self):
        """
        Will lookup if exists the previous saved position looked up.
        Will also check that the age of the file is older than the last look as
        the file may have been replaced via log rotation.
        """
        query = """
            SELECT skip_bytes
              FROM %s_position
             WHERE log_file = ? AND log_time = ? AND tag = ?""" % self.name
        for row in self.local_db.execute(
                query, [self.log_file, self.log_time, self.tag]):
            return row[0]

        return 0

    def set_since(self):
        """
        Sets the since for the last email for the log_file/log_time.
        """
        self.local_db.execute("""
            UPDATE OR IGNORE %s_email
               SET transaction_time =
                       (SELECT max(transaction_time)
                         FROM %s_logs
                        WHERE log_file = ?
                          AND log_time = ?
                          AND tag = ?)
             WHERE log_file = ?
               AND log_time = ?
               AND tag = ?"""
                              % (self.name, self.name),
                              [self.log_file, self.log_time, self.tag,
                               self.log_file, self.log_time, self.tag])
        changes = 0
        for change in self.local_db.execute("SELECT changes()"):
            changes += change[0]
        if changes == 0:
            self.local_db.execute("""
                INSERT or IGNORE INTO
                %s_email(log_file, log_time, transaction_time, tag)
                SELECT log_file, log_time, max(transaction_time), tag
                  FROM %s_logs
                 WHERE log_file = ?
                   AND log_time = ?
                   AND tag = ?"""
                                  % (self.name, self.name),
                                  [self.log_file, self.log_time, self.tag])
        self.local_db.commit()

    def set_skip_bytes(self, skip_bytes):
        """
        Sets the skip_bytes for the log_file/log_time.
        :param skip_bytes: What to set to in the db.
        """
        self.local_db.execute("""
            UPDATE %s_position
               SET skip_bytes = ?
             WHERE log_file = ?
               AND log_time = ?
               and tag = ?"""
                              % self.name,
                              [skip_bytes, self.log_file, self.log_time,
                               self.tag])
        self.local_db.execute("""
            INSERT INTO %s_position(log_file, log_time, tag, skip_bytes)
            SELECT ?, ?, ?, ?
             WHERE (Select Changes() = 0)""" % self.name,
                              [self.log_file, self.log_time, self.tag,
                               skip_bytes])
        self.local_db.commit()

    def store_new(self):
        """
        Parses the logs from the last position and adds to the local db.
        """
        for row in self._new_rows():
            self.local_db.execute("""
                INSERT INTO
                %s_logs(log_file, log_time, transaction_time, tag, message)
                VALUES(?, ?, ?, ?, ?)""" % self.name,
                                  [self.log_file, self.log_time,
                                   row.transaction_time,
                                   row.tag, msgpack.packb(row.message)])
            self.local_db.commit()

    def _init_local_db(self):
        """
        Create all of the tables needed for this named parser.
        """
        self.local_db.execute("""
            CREATE TABLE IF NOT EXISTS %s_email(
                log_file TEXT NOT NULL,
                log_time REAL NOT NULL,
                transaction_time TEXT NOT NULL,
                tag TEXT NOT NULL)""" % self.name)
        self.local_db.execute("""
            CREATE TABLE IF NOT EXISTS %s_position(
                log_file TEXT NOT NULL,
                log_time REAL NOT NULL,
                tag TEXT NOT NULL,
                skip_bytes INT NOT NULL DEFAULT 0)""" % self.name)
        self.local_db.execute("""
            CREATE TABLE IF NOT EXISTS %s_logs(
                log_file TEXT NOT NULL,
                log_time FLOAT NOT NULL,
                transaction_time TEXT NOT NULL,
                tag TEXT NOT NULL,
                message BLOB)""" % self.name)
        self.local_db.commit()

    def _new_rows(self):
        """
        Opens the log file and yields LogRow object for matching tag.
        """
        since = self.since
        if since is None:
            since = self.get_since()
        since = date_parse(since)
        try:
            with open(self.log_file, 'r') as log_file:
                timestamp = None
                message = None
                log_file.seek(self.get_skip_bytes())
                for line in log_file:
                    if re.match(self.log_date_regex, line):
                        if message is not None:
                            yield LogRow(self.log_file, timestamp, self.tag,
                                         message)
                            message = None
                        matcher = re.match(self.head_regex, line)
                        if matcher:
                            timestamp = date_parse(matcher.group(1))
                            if timestamp > since:
                                message = matcher.group(3)
                    elif message is not None:
                        message += line
                if message is not None:
                    yield LogRow(self.log_file, timestamp, self.tag, message)
                self.set_skip_bytes(log_file.tell())
        except Exception:
            raise

    def _rows(self):
        """
        Pulls row data from the local db.
        """
        since = self.since
        if since is None:
            since = self.get_since()
        for row in self.local_db.execute("""
                SELECT log_file, transaction_time, tag, message
                  FROM %s_logs
                 WHERE log_file = ?
                   AND log_time = ?
                   AND tag = ?
                   AND transaction_time > ?""" % self.name,
                                         [self.log_file, self.log_time,
                                          self.tag, since]):
            yield LogRow(row[0], row[1], row[2],
                         msgpack.unpackb(row[3], encoding='utf-8'))
        if self.since is None:
            self.set_since()

    __del__ = __exit__ = cleanup
