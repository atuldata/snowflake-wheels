"""
All common settings
"""
import platform
import os
import yaml

APP_ROOT = os.environ.get('APP_ROOT', 'PWD')
CONF_ROOT = os.path.join(APP_ROOT, 'conf')
LOCAL_OUTPUT = os.path.join(APP_ROOT, 'output')
if not os.path.exists(LOCAL_OUTPUT):
    os.makedirs(LOCAL_OUTPUT)

LOG_DIRECTORY = os.path.join(APP_ROOT, 'logs')
if not os.path.exists(LOG_DIRECTORY):
    os.makedirs(LOG_DIRECTORY)

LOG_DATE_FORMAT = '%Y-%m-%d %H:%M:%S'
LOG_DATE_REGEX = r'^\[(\d{4}-\d{2}-\d{2}\ \d{2}:\d{2}:\d{2})\]'

HOSTNAME = platform.node()
TMP = os.environ.get('TMP', '/tmp')


def get_config(name, path=None):
    """
    Reads in the yaml config from the CONF_ROOT
    path: Is the relative path from CONF_ROOT
    name: Is the name of the file without the extension.
    Currently all conf are yaml.
    rtype: dict
    """
    conf_dir = CONF_ROOT
    if path is not None:
        conf_dir = os.path.join(conf_dir, path)
    config_file = os.path.join(conf_dir, '.'.join([name, 'yaml']))
    if os.path.exists(config_file):
        with open(config_file) as conf:
            return yaml.load(conf)

    # If the config_file doesn't exist just return an emtpy dict aas a dict
    # response is expected.
    return {}


ENV = get_config('env')
