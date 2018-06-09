"""
For setup of ODFI ETL.
"""
import sys
from setuptools import setup, find_packages

INSTALL_REQUIRES = {
    'python-dateutil': '>=2.5.3,<3',
    'pid': '>=2.1.1',
    'retrying': '>=1.3.3,<2',
    'snowflake-connector-python': '>=1.5.1,<2',
    'ox-dw-db': '>=0.0.5',
    'ox-dw-logger': '>=0.0.1',
    'ox-dw-load-state': '>=0.0.1',
    'ox-dw-odfi-client': '>=0.0.4'
}
if sys.version_info < (2, 7):
    INSTALL_REQUIRES.update({'argparse': '==1.2.1', 'ordereddict': '>=1.1'})

setup(
    name='ox_dw_snowflake_odfi_etl',
    version=open('VERSION').read().strip(),
    description=__doc__,
    long_description=open("README.md").read(),
    author='dw-scrum-team',
    author_email='dw-scrum-team@openx.com',
    packages=find_packages(),
    package_data={'ox_dw_snowflake_odfi_etl': ['app_config/*.yaml']},
    install_requires=["%s%s" % (k, v) for k, v in INSTALL_REQUIRES.items()],
    entry_points={
        'console_scripts': ['odfi_etl=ox_dw_snowflake_odfi_etl.commands:main']
    },
    classifiers=[
        'Development Status :: 4 - Beta', 'Intended Audience :: Developers',
        'Operating System :: MacOS :: MacOS X',
        'Operating System :: POSIX :: Linux',
        'Programming Language :: Python :: 2.6',
        'Programming Language :: Python :: 3.5',
        'Topic :: Software Development :: Libraries :: Python Modules'
    ])
