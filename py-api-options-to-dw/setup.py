"""
Setup for ox-dw-api-options-to-dw.
"""
import sys
from setuptools import setup, find_packages

INSTALL_REQUIRES = {
    'unicodecsv': '>=0.14.1',
    'PyYAML': '>=3.11,<4',
    'ox_dw_db': '>=0.0.5',
    'ox_dw_load_state': '>=0.0.2',
    'ox_dw_logger': '>=0.0.1',
    'requests': '>=2.10.0,<3'
}
if sys.version_info < (2, 7):
    INSTALL_REQUIRES.update({
        'argparse': '>=1.2.1',
        'ordereddict': '>=1.1'
    })

setup(
    name='ox_dw_snowflake_api_options_to_dw',
    version=open('VERSION').read().strip(),
    description=__doc__,
    long_description=open("README.md").read(),
    author='dw-scrum-team',
    author_email='dw-scrum-team@openx.com',
    packages=find_packages(),
    install_requires=["%s%s" % (k, v) for k, v in INSTALL_REQUIRES.items()],
    entry_points={
        'console_scripts': [
            'api_options_to_dw=ox_dw_snowflake_api_options_to_dw.commands:api_options_to_dw'
        ]
    }
)
