"""
Utilities for validating the SalesForce data.
"""


def check_val(val, field_type):
    """
    Returns properly quoted value.
    """
    if val is None:
        return ''
    if field_type == 'boolean':
        return '1' if val else '0'
    if field_type == 'address':
        return \
            " ".join([check_address_val(v) for v in val.values()])
    if field_type in ('double', 'percent', 'currency', 'int'):
        return str(val)
    if field_type in ('datetime', 'date', 'timestamp'):
        return val.replace('T', ' ').replace('+', '')
    return val.replace(',', ';').replace('\\', '\\\\').replace('"', '\'').replace(chr(30), '')


def check_address_val(val):
    """
    Escapes quotes if needed.
    """
    return '' if val is None else val.replace('"', '\\"')


def salesforce_type_to_vsql_type(
        field_type, field_length, field_precision, field_scale):
    """
    Returns the vsql field type.
    """
    if field_type in ['datetime', 'date', 'boolean', 'int']:
        return field_type.upper()
    if field_type in ['double', 'percent', 'currency']:
        return "NUMERIC(%s, %s)" % (field_precision, field_scale)
    if field_type == 'timestamp':
        return "TIMESTAMP_NTZ"
    if field_type == 'address':
        return "VARCHAR(500)"
    if field_type == 'textarea':
        return "VARCHAR(32768)"
    if field_length > 65000:
        return "VARCHAR(%s)" % 65000

    return "VARCHAR(%s)" % field_length
