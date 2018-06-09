"""
Actions built automatically from the actions folder
"""
import os
import importlib

ACTIONS = {}
for py_file in os.listdir(os.path.join(os.path.dirname(__file__))):
    if py_file.endswith('.py') and not py_file.startswith('_'):
        name = os.path.splitext(py_file)[0]
        module = importlib.import_module('.' + name,
                                         'ox_dw_snowflake_odfi_etl.actions')
        ACTIONS.update({
            name: {
                "help": module.__doc__,
                "options": getattr(module, 'OPTIONS')
            }
        })
