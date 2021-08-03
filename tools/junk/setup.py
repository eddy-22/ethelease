#!/usr/bin/env python

import sys
from setuptools import setup


if sys.version_info < (3, 9):
    print('`ethelease` doesn\'t support this version of Python!')
    print('Please upgrade to Python 3.9 or higher...')
    sys.exit(1)


try:
    from setuptools import find_namespace_packages
except ImportError:
    print('`ethelease` requires setuptools v40.1.0 or higher.')
    print('Please upgrade setuptools with "pip install --upgrade setuptools" ')
    sys.exit(1)


NAME, VERSION = 'ethelease', '0.0.0'


setup(
    author='William Rex Chen',
    install_requires=[
        f'{NAME}-core=={VERSION}',
        'boto3==1.17.87',
        'psycopg2-binary==2.9.1',
        'PyMySQL==0.9.3',
    ],
    name=f'{NAME}-junk',
    packages=find_namespace_packages(
        include=[
            f'{NAME}',
            f'{NAME}.*',
        ]
    ),
    version=VERSION,
)
