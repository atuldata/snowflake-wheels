"""
This converter looks through oxtl list and finds the comparisons.
"""
from collections import defaultdict
from pyparsing import (CaselessLiteral, Combine, Forward, Group, Keyword,
                       Optional, ParseException, ParseResults, Word,
                       ZeroOrMore, alphanums, alphas, delimitedList, nums,
                       oneOf, quotedString)
from .logger import get_logger

IDENT = Word(alphas, alphanums + "_$").setName("identifier")
FIELD_NAME = (delimitedList(IDENT, ".", combine=True)).setName("column name")
FIELD_NAME_LIST = Group(delimitedList(FIELD_NAME))

# The operators
NOT_ = Keyword('not', caseless=True)
AND_ = Keyword('and', caseless=True)
OR_ = Keyword('or', caseless=True)
IN_ = Keyword('intersects', caseless=True)

E = CaselessLiteral("E")
EQ = '='
LOGICAL_OPS = ['and', 'or']
NE = '!='
NE_OPS = [NE, '!~', '>', '<']
NOT = 'not'
PARENTHESES = ['(', ')']
BINOP = \
    oneOf(
        "== %s =? =^ =$ =~ !~ intersects >= > <= < before after" % NE,
        caseless=True)
ARITH_SIGN = Word("+-", exact=1)
REAL_NUM = \
    Combine(
        Optional(ARITH_SIGN) +
        (Word(nums) + "." + Optional(Word(nums)) | ("." + Word(nums))) +
        Optional(E + Optional(ARITH_SIGN) + Word(nums)))
INT_NUM = \
    Combine(
        Optional(ARITH_SIGN) + Word(nums) +
        Optional(E + Optional("+") + Word(nums)))

R_VALUE = REAL_NUM | INT_NUM | quotedString | FIELD_NAME

OXTL_EXPRESSION = Forward()
OXTL_CLAUSE = Group((FIELD_NAME + BINOP + R_VALUE) | (
    FIELD_NAME + IN_ + PARENTHESES[0] + delimitedList(R_VALUE) + PARENTHESES[1]
) | (FIELD_NAME + IN_ + PARENTHESES[0] + Forward() + PARENTHESES[1]) | (
    PARENTHESES[0] + OXTL_EXPRESSION + PARENTHESES[1]) | (
        NOT_ + OXTL_EXPRESSION))
# This does seem to work. << is apparently overloaded.
OXTL_EXPRESSION << OXTL_CLAUSE + \
    ZeroOrMore((AND_ | OR_ | IN_) + OXTL_EXPRESSION)

COLUMNS = ['pub.custom.', 'ox.geo.', 'ox.techno.', 'ox.viewability.']
MAX_FIELD_LENGTH = 256
OUTPUT_FORMAT = '%s%s %s %s'


def get_converted_rows(rows):
    """
    Takes in a iterator of lists and yields one converted list as a time.
    The yielded rows will be greater by 3.
    """
    for row in rows:
        # Shortcut the parsing on those lines that do not need it to decrease
        # the number of parsing errors.
        if not any(col in row[5] for col in COLUMNS):
            yield row
        else:
            for _ in _get_row(row):
                yield _


def _get_row(row):
    """
    Takes a single row and returns it with the additional 3 items.
    """
    logger = get_logger()
    try:
        eql = dict((key, defaultdict(set)) for key in COLUMNS)
        not_eql = dict((key, defaultdict(set)) for key in COLUMNS)
        process_oxtl(OXTL_EXPRESSION.parseString(row[5]), eql, not_eql)
        for index, col in enumerate(COLUMNS, start=6):
            output = ''
            for container, prefix in [(eql, EQ), (not_eql, NE)]:
                for vkey in sorted(container[col]):
                    vval = sorted(container[col][vkey])
                    if len(vval):
                        output += \
                            OUTPUT_FORMAT % (
                                ' ' if len(output) else '',
                                vkey,
                                prefix,
                                '(%s)' % ','.join(vval))
            if len(output):
                row[index] = output[:MAX_FIELD_LENGTH]
        yield row
    except ParseException as exc:
        logger.warning('ParseException:%s; %s', row[5], exc)
    except RuntimeError as exc:
        # Use RecursionError in python 3.5
        if 'maximum recursion' in str(exc):
            logger.warning('RecursionError:%s;', row)
        else:
            logger.error('UNHANDLED RuntimeError:%s;%s', row, exc)
            raise
    except Exception as exc:
        logger.error('UNHANDLED Exception:%s;%s', row, exc)
        raise


def process_oxtl(parsed_oxtl, eql, not_eql):
    """
    Updates parsed_oxtl, eql and not_eql with the parsed values.
    """
    iterator = iter(parsed_oxtl)
    for element in iterator:
        if isinstance(element, ParseResults):
            process_oxtl(element, eql, not_eql)
        elif element is NOT:
            # Switch the two containers because this is a negation
            try:
                process_oxtl(next(iterator), not_eql, eql)
                continue
            except StopIteration:
                break
        elif any(element in items for items in [LOGICAL_OPS, PARENTHESES]):
            # We don't care about logical operators or parentheses
            continue
        else:
            # At this point we should have an expression: FIELD_NAME OP rval
            try:
                operator = next(iterator)
                rval = next(iterator)
                dest = not_eql if operator in NE_OPS else eql
                for dest_key, dest_value in dest.items():
                    if element.startswith(dest_key):
                        dest_value[element[len(dest_key):]].add(rval)
            except StopIteration:
                break
