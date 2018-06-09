"""
For logger setup
"""

from setuptools import setup, find_packages

setup(
    name='ox_dw_snowflake_sql_runner',
    version=open('VERSION').read().strip(),
    description=__doc__,
    long_description=open('README.md').read(),
    author='dw-scrum-team',
    author_email='dw-scrum-team@openx.com',
    packages=find_packages(),
    install_requires=[
        'ox_dw_db>=0.0.3',
        'ox_dw_load_state>=0.0.2',
        'ox_dw_logger>=0.0.1'
    ],
    entry_points={
        'console_scripts': [
            'sql_runner=ox_dw_snowflake_sql_runner.commands:sql_runner',
        ]
    },
    classifiers=[
        'Operating System :: MacOS :: MacOS X',
        'Operating System :: POSIX :: Linux',
        'Programming Language :: Python :: 3.5',
        'Topic :: Software Development :: Libraries :: Python Modules',
        'Topic :: Database :: Snowflake'
    ]
)
