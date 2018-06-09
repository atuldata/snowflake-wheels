"""
Common utilities for use with odfi etl.
"""
import re

PARAMS_EXTRACTOR = re.compile(r'\?\(\w+\)\?', re.DOTALL)
PARAM_STYLES = set(['qmark', 'numeric', 'format', 'pyformat'])


def get_param(paramstyle, index=None, stmt=None, start=0, end=-1):
    """
    Return the appropriate param for the given style and index.
    """
    if paramstyle == 'qmark':
        return '?'
    if paramstyle == 'numeric' and index is not None:
        return ':' + str(index)
    if paramstyle == 'format':
        return '%s'
    if paramstyle == 'pyformat':
        return '%(' + stmt[start + 2:end - 2] + ')s'
    raise ValueError("The only supported paramstyle values are %s",
                     PARAM_STYLES)


def stmt_with_args(orig_stmt, kwargs, paramstyle='qmark'):
    """
    Pass in a SQL statment with variables in this format: ?(key)?
    to have replaced with the server side param.
    :param orig_stmt: The SQL stmt with the replacement tags.
    :param kwargs: A Dictionary with the key/value pairs for the substitutions.
    :param paramstyle: For qmark or numeric style.
    """
    args = {} if paramstyle == 'pyformat' else []
    stmt = orig_stmt
    offset = 0
    for index, match in enumerate(PARAMS_EXTRACTOR.finditer(orig_stmt), 1):
        start, end = [index - offset for index in match.span()]
        param = get_param(
            paramstyle, index=index, stmt=stmt, start=start, end=end)
        if paramstyle != 'pyformat':
            args.append(kwargs[stmt[start + 2:end - 2]])
        stmt = param.join([stmt[:start], stmt[end:]])
        offset += end - start - len(param)
    if paramstyle == 'pyformat':
        return stmt, kwargs

    return stmt, tuple(args)
