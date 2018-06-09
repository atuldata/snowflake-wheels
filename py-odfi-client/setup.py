"""
For setup of ODFI Client.
"""
import os
import sys
from setuptools import setup, find_packages

INSTALL_REQUIRES = {
    'requests': '>=2.10.0,<3',
    'retrying': '>=1.3.3,<2',
    'xmltodict': '>=0.11.0,<1'
}
if sys.version_info < (2, 7):
    INSTALL_REQUIRES.update({
        'argparse': '>=1.2.1',
        'ordereddict': '>=1.1'
    })


setup(
    name='ox_dw_odfi_client',
    version=open('VERSION').read().strip(),
    description=__doc__,
    long_description=open("README.md").read(),
    author='dw-scrum-team',
    author_email='dw-scrum-team@openx.com',
    packages=find_packages(),
    install_requires=["%s%s" % (k, v) for k, v in INSTALL_REQUIRES.items()],
    classifiers=[
        'Development Status :: 4 - Beta',
        'Intended Audience :: Developers',
        'Operating System :: MacOS :: MacOS X',
        'Operating System :: POSIX :: Linux',
        'Programming Language :: Python :: 2.6'
        'Programming Language :: Python :: 3.5',
        'Topic :: Software Development :: Libraries :: Python Modules'
    ]
)
