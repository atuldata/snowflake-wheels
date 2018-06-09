"""
Utilities for generating SQL from the odfi schemas.
"""
import os
from ox_dw_odfi_client import get_frequency
from .settings import COMPRESSION, STAGE_NAME

TIMESTAMP_FORMATS = {'HOURLY': 'YYYY-MM-dd_HH', 'DAILY': 'YYYY-MM-dd'}
MYSQL_TO_DW = {
    'datetime': 'DATETIME',
    'date': 'DATE',
    'boolean': 'BOOLEAN',
    'int unsigned': 'INT',
    'integer unsigned': 'INT',
    'smallint unsigned': 'INT',
    'byte': 'INT',
    'bigint': 'BIGINT',
    'bigint unsigned': 'BIGINT',
}
MYSQL_DATETIME_FORMATS = {
    "datetime": "FORMAT 'YYYY-MM-dd_HH'",
    "date": "FORMAT 'YYYY-MM-dd'"
}


def copy_sql(dataset, stage_table_name):
    """
    For a given dataset, target table and field mapping a copy statement
    is returned.
    """
    return """
        COPY INTO %(table_name)s
        FROM @%(stage_name)s/%(stage_path)s
        FILE_FORMAT = (
            TYPE = CSV
            COMPRESSION = '%(compression)s'
            FIELD_DELIMITER = '%(field_delimiter)s'
            SKIP_HEADER = %(skip)s
            TRIM_SPACE = TRUE
            ERROR_ON_COLUMN_COUNT_MISMATCH = TRUE
            DATE_FORMAT = 'YYYY-MM-dd'
            TIMESTAMP_FORMAT = '%(timestamp_format)s'
        )
    """ % {
        'table_name': stage_table_name,
        'stage_name': STAGE_NAME,
        'stage_path': os.path.join(dataset.feed_name, str(dataset.serial)),
        'compression': COMPRESSION,
        'field_delimiter': dataset.schema.get('@fs'),
        'skip': dataset.schema.get('@skip_lines', 0),
        'timestamp_format':
        TIMESTAMP_FORMATS.get(get_frequency(dataset.meta['readableInterval']))
    }


def create_sql(schema, stage_table_name):
    """
    For a given schema, target table and field mapping a create table statement
    is returned.
    """
    return """
        CREATE TEMPORARY TABLE IF NOT EXISTS %(table_name)s(
            %(fields)s
        )
    """ % {
        'table_name': stage_table_name,
        'fields': ',\n            '.join(_get_fields(schema))
    }


def _get_fields(schema):
    """
    Returns list of mapped fields for the given schema.
    """
    return [
        ' '.join([
            field['@name'], (MYSQL_TO_DW.get(field['@mysql_type'],
                                             field['@mysql_type'])).upper()
        ]) for field in schema.get('field_list').get('field')
    ]
