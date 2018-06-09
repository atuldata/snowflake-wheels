"""
Setup for API Extractor
"""
from setuptools import setup, find_packages


setup(
    name='ox_dw_snowflake_api_to_dw',
    version=open('VERSION').read().strip(),
    description='RabbitMQ Consumer that outputs files for load into the DW.',
    long_description=open("README.md").read(),
    author='dw-scrum-team',
    author_email='dw-scrum-team@openx.com',
    packages=find_packages(),
    include_package_data=True,
    install_requires=[
        'msgpack-python>=0.4.8',
        'ox-dw-db>=0.0.5',
        'ox-dw-logger>=0.0.1',
        'ox-dw-load-state>=0.0.2',
        'ujson>=1.35,<2.0',
        'ox_rabbit_mq>=0.0.1,<1.0',
        'progressbar2>=3.34.0,<4',
        'psycopg2==2.7.3.1',
        'pyparsing>=2.1.10,<3',
        'retrying>=1.3.3,<2'
    ],
    entry_points={
        'console_scripts': [
            'api_consumer=ox_dw_snowflake_api_to_dw.commands:api_consumer',
            'api_loader=ox_dw_snowflake_api_to_dw.commands:api_loader',
            'api_harmonizer=ox_dw_snowflake_api_to_dw.commands:api_harmonizer',
            'api_duplicate_report=ox_dw_snowflake_api_to_dw.commands:api_duplicate_report'
        ]
    }
)
