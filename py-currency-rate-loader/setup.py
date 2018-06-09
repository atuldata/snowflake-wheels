"""
For setup of Currency Rate Loader.
"""
from setuptools import setup, find_packages


setup(
    name='ox_dw_snowflake_currency_rate_loader',
    version=open('VERSION').read().strip(),
    description=__doc__,
    long_description=open("README.md").read(),
    author='dw-scrum-team',
    author_email='dw-scrum-team@openx.com',
    packages=find_packages(),
    install_requires=[
        'ox_dw_db>=0.0.5',
        'ox_dw_load_state>=0.0.2',
        'ox_dw_logger>=0.0.1',
        'unicodecsv>=0.14.1,<1',
        'python-dateutil>=2.5.3,<3',
        'requests>=2.10.0,<3',
        'retrying>=1.3.3,<2'
    ],
    entry_points={
        'console_scripts': [
            'currency_rate_loader=ox_dw_snowflake_currency_rate_loader.commands:main'
        ]
    },
    classifiers=[
        'Intended Audience :: Developers',
        'Operating System :: MacOS :: MacOS X',
        'Operating System :: POSIX :: Linux',
        'Programming Language :: Python :: 2.6'
        'Programming Language :: Python :: 3.5',
        'Topic :: Software Development :: Libraries :: Python Modules',
        'Topic :: Database :: Snowflake'
    ]
)
