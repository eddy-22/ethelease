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


NAME, VERSION, URL = 'ethelease', '0.0.0', 'https://github.com/WillemRvX/ethelease'


with open("README.md", "r", encoding="utf-8") as fh:
    LONG_DESCRIPTION = fh.read()


setup(
    author='William Rex Chen',
    install_requires=[
        f'{NAME}-core=={VERSION}',
        f'{NAME}-tools=={VERSION}',
        'wheel==0.36.2',
    ],
    long_description=LONG_DESCRIPTION,
    name=NAME,
    packages=find_namespace_packages(
        exclude=[
            'tests',
            'tests.*',
        ]
    ),
    url=URL,
    version=VERSION,
)
