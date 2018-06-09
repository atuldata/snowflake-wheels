"""
Common utilities.
"""
from collections import Mapping, Set, Sequence
import os
import sys
import warnings


def default_iterator(mapping):
    """
    Use this iterator for Mapping objects.
    """
    return getattr(mapping, 'iteritems', mapping.items)()


def generate_rows(obj, cols):
    """
    This will automatically expand lists of dictionaries.
    For getting elements from dot notated dicts pass in a walked obj.
    Current implementation only works if the object only has one or the
    other. Examine the output since lists will multiply the rows yielded.
    """

    def _generate_rows(working_obj, working_cols, working_row=None):
        if working_row is None:
            working_row = []
        for index, col_name in enumerate(working_cols):
            if col_name in working_obj:
                if isinstance(working_obj.get(col_name), (list, set)):
                    for item in working_obj.get(col_name):
                        working_obj[col_name] = item
                        for _ in _generate_rows(
                                working_obj, working_cols[index:],
                                working_row[:]):
                            yield _
                else:
                    working_row.append(working_obj.get(col_name))
            else:
                # We can handle lists here
                idx = 0
                while idx >= 0:
                    new_obj = working_obj.copy()
                    cna, sep, cna_obj = col_name.partition('.')
                    keys = [key for key in sorted(working_obj.keys())
                            if key.startswith('%s.%s' % (cna, str(idx)))]
                    if bool(keys):
                        idx += 1
                        for key in keys:
                            cna, sep, _idx = key.partition('.')
                            if '.' in _idx:
                                _idx, sep, cna_obj = _idx.partition('.')
                                new_obj[sep.join([cna, cna_obj])] = \
                                    working_obj.get(key)
                            else:
                                new_obj[col_name] = working_obj.get(key)
                        for _ in _generate_rows(
                                new_obj, working_cols[index:], working_row[:]):
                            yield _
                    else:
                        if idx == 0:
                            lidx = 0
                            is_list = False
                            while True:
                                lcol_name = '.'.join([col_name, str(lidx)])
                                if lcol_name in new_obj:
                                    is_list = True
                                    new_obj[col_name] = \
                                        working_obj.get(lcol_name)
                                    for _ in _generate_rows(
                                            new_obj,
                                            working_cols[index:],
                                            working_row[:]):
                                        yield _
                                    lidx += 1
                                else:
                                    break
                            if not is_list:
                                # This was empty after all
                                working_row.append(None)
                        idx = -1

        if len(working_row) >= len(working_cols):
            yield working_row

    for row in _generate_rows(get_walked_obj(obj), cols):
        if len(row) == len(cols):
            yield row


def get_walked_obj(in_obj):
    """
    Uses objwalk above and returns a dict of dot notated keys.
    """
    out_obj = {}
    for paths, obj in objwalk(in_obj):
        out_obj['.'.join([str(path) for path in paths])] = obj

        part_key = '.'.join([str(path) for path in paths[:-1]])
        for key, value in [
                ('.'.join([part_key, 'KEYS']), paths[-1]),
                ('.'.join([part_key, 'VALUES']), obj)]:
            if key not in out_obj:
                out_obj[key] = []
            out_obj[key].append(value)

    return out_obj


def is_empty_file(fpath):
    """
    If the file exists and has bytes then True else False.
    """
    return \
        fpath is not None and \
        os.path.isfile(fpath) and \
        os.path.getsize(fpath) == 0


def objwalk(obj, path=(), memo=None):
    """
    Found this here:
    http://code.activestate.com/recipes/577982-recursively-walk-python-objects
    """
    if memo is None:
        memo = set()
    iterator = None
    if isinstance(obj, Mapping):
        iterator = default_iterator
    elif isinstance(obj, (Sequence, Set)) and \
            not isinstance(obj, (str, bytes)):
        iterator = enumerate
    if iterator:
        if id(obj) not in memo:
            memo.add(id(obj))
            for path_component, value in iterator(obj):
                for _ in objwalk(
                        value, path + (str(path_component),), memo):
                    yield _
            memo.remove(id(obj))
    else:
        yield path, obj


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
