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
    entry_points=dict(
        console_scripts=[
            f'ethel = ethelease.makeitso.main_event:main'
        ],
    ),
    include_package_data=True,
    install_requires=[
        'croniter==1.0.11',
        'docker==5.0.0',
        'google-api-core==1.29.0',
        'google-api-python-client==2.7.0',
        'google-auth==1.30.1',
        'Jinja2==3.0.1',
        'kubernetes==17.17.0',
        'oauthlib==3.1.0',
        'pydash==4.7.6',
        'python-dotenv==0.17.1',
        'python-http-client==3.2.7',
        'PyYAML==5.4.1',
        'tzlocal==2.1',
    ],
    name=f'{NAME}-core',
    packages=find_namespace_packages(
        include=[
            f'{NAME}',
            f'{NAME}.*',
        ]
    ),
    version=VERSION,
)
