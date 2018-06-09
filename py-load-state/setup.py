"""
For setup of Load State Interface.
"""
from setuptools import setup, find_packages


setup(
    name='ox_dw_load_state',
    version=open('VERSION').read().strip(),
    description=__doc__,
    long_description=open("README.md").read(),
    author='dw-scrum-team',
    author_email='dw-scrum-team@openx.com',
    packages=find_packages(),
    install_requires=[
        'ox-dw-db>=0.0.3'
    ],
    classifiers=[
        'Operating System :: MacOS :: MacOS X',
        'Operating System :: POSIX :: Linux',
        'Programming Language :: Python :: 2.6'
        'Programming Language :: Python :: 3.5',
        'Topic :: Software Development :: Libraries :: Python Modules'
    ]
)

