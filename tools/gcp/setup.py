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
        'google-api-core==1.29.0',
        'google-api-python-client==2.7.0',
        'google-auth==1.30.1',
        'google-cloud-bigquery==2.18.0',
        'google-cloud-core==1.6.0',
        'google-cloud-storage==1.38.0',
    ],
    name=f'{NAME}-gcptools',
    packages=find_namespace_packages(
        include=[
            f'{NAME}',
            f'{NAME}.*',
        ]
    ),
    version=VERSION,
)
