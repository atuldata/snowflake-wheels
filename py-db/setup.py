"""
For database connector setup
"""

from setuptools import setup, find_packages

setup(
    name='ox_dw_db',
    version=open('VERSION').read().strip(),
    description=__doc__,
    long_description=open('README.md').read(),
    author='dw-scrum-team',
    author_email='dw-scrum-team@openx.com',
    packages=find_packages(),
    install_requires=[
        'psycopg2==2.7.3.1',
        'pyodbc>=3.0.10,<4',
        'snowflake-connector-python>=1.5.2,<2'
    ],
    classifiers=[
        'Development Status :: 3 - Alpha',
        'Operating System :: MacOS :: MacOS X',
        'Operating System :: POSIX :: Linux',
        'Programming Language :: Python :: 3.5',
        'Topic :: Software Development :: Libraries :: Python Modules'
    ]
)
