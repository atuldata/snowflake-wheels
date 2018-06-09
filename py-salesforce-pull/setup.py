"""
Setup for salesforce_pull
"""
from setuptools import setup, find_packages


setup(
    name='ox_dw_snowflake_salesforce_pull',
    version=open('VERSION').read().strip(),
    description='Pull salesforce data into snowflake database.',
    long_description=open("README.md").read(),
    author='dw-scrum-team',
    author_email='dw-scrum-team@openx.com',
    packages=find_packages(),
    install_requires=[
        'ox-dw-db>=0.0.5',
        'ox-dw-load-state>=0.0.2',
        'ox_dw_logger>=0.0.1',
        'retrying>=1.3.3,<2',
        'simple-salesforce>=0.72.2'
    ],
    entry_points={
        'console_scripts': [
            'salesforce_pull=ox_dw_snowflake_salesforce_pull:import_objects'
        ]
    },
    classifiers=[
        'Operating System :: MacOS :: MacOS X',
        'Operating System :: POSIX :: Linux',
        'Programming Language :: Python :: 2.6'
        'Programming Language :: Python :: 3.5',
        'Topic :: Software Development :: Libraries :: Python Modules',
        'Topic :: Database :: Snowflake'
    ]
)
