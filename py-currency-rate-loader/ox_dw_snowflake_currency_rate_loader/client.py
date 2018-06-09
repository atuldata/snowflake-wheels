"""
Essentially the client for fetching the currency rates from the rest api.
"""
import sys
from dateutil.parser import parse as parse_date
import requests
from unicodecsv import DictReader
from .settings import BASE_URL, ENV
from .exceptions import UserException


class Client(object):
    """
    An iterator for all of the exchange rates from the currency service for a
    given base_currency and date.
    Note the the date in the results is the date given not what is returned
    from the currency service. Also one extra row is returned for the 1:1 rate.
    """

    def __init__(self, base_currency, date, fields=None):
        self.base_currency = base_currency
        self.date_str = date.strftime('%Y-%m-%d')
        self._fields = fields
        self._reader = None

    @property
    def fields(self):
        """
        An ordered list of the fields names for the return values.
        """
        if self._fields is None:
            self._fields = self.reader.fieldnames
            self._fields.append('updated_date')

        return self._fields

    @property
    def one_to_one(self):
        """
        The missing 1:1 currency exchange rate for the given base_currency.
        """
        return {
            'base_currency': self.base_currency,
            'currency': self.base_currency,
            'date': self.date_str,
            'updated_date': self.date_str,
            'bid': 1,
            'ask': 1
        }

    @property
    def reader(self):
        """
        The csv reader.
        :returns csv.DictReader:
        """
        if self._reader is None:
            response = requests.get(self.url)
            if response.status_code == 403:
                raise UserException("Access forbidden error. url %s" % self.url)
            if response.status_code == 400:
                raise UserException(response.content)
            if response.status_code == 200:
                self._reader = DictReader(response.iter_lines())
            else:
                raise Exception("Accessing url %s failed with "
                                "status code %s" % (self.url, response.status_code))
        return self._reader

    @property
    def url(self):
        """
        The assembled url for the currency service.
        """
        return \
            "%s%s.csv?api_key=%s&date=%s" % (
                BASE_URL, self.base_currency,
                ENV.get('CURRENCY_WS_CLIENT'), self.date_str)

    def __iter__(self):
        """
        Yields the currency rates in  for the given base_currency.
        """
        yield [self.one_to_one.get(field) for field in self.fields]
        for row in self.reader:
            try:
                row['updated_date'] = parse_date(
                    row['date']).strftime('%Y-%m-%d')
                row['date'] = self.date_str
            except KeyError as error:
                sys.stderr.write(
                    'This row is missing date: %s; from url %s\n' % (str(row),
                                                                     self.url))
                raise
            yield [row.get(field) for field in self.fields]
