"""
For logger setup
"""

from setuptools import setup, find_packages

setup(
    name='ox_dw_logger',
    version=open('VERSION').read().strip(),
    description=__doc__,
    long_description=open('README.md').read(),
    author='dw-scrum-team',
    author_email='dw-scrum-team@openx.com',
    packages=find_packages(),
    install_requires=[
        'python-dateutil>=2.5.3,<3',
        'pyYAML>=3.11,<4',
        'pTable>=0.9.2,<1',
        'msgpack-python>=0.4.8,<1'
    ],
    entry_points={
        'console_scripts': [
            'etl_logs_print_report=ox_dw_logger.commands:etl_logs_print_report',
            'etl_logs_email_report=ox_dw_logger.commands:etl_logs_email_report'
        ]
    },
    classifiers=[
        'Operating System :: MacOS :: MacOS X',
        'Operating System :: POSIX :: Linux',
        'Programming Language :: Python :: 3.5',
        'Topic :: Software Development :: Libraries :: Python Modules'
    ]
)
