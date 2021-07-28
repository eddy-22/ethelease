#!/usr/bin/env python

import argparse
import json
import logging
import multiprocessing as mp
import os
import sys
from functools import wraps
from os.path import expanduser
from time import sleep
from dotenv import load_dotenv


ENV, PATH_SECRETS = os.environ.get('_ENV_', 'dv'), os.environ.get('PATH_SECRETS', '/etc/secrets')


LOGGER = logging.getLogger()
LOGGER.setLevel(logging.INFO)
FORMAT = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
HANDLES = logging.StreamHandler(sys.stdout)
HANDLES.setLevel(logging.INFO)
HANDLES.setFormatter(FORMAT)
LOGGER.addHandler(HANDLES)


def args() -> dict:
    parser = argparse.ArgumentParser()
    parser.add_argument('--Args')
    args = parser.parse_args()
    args = args.Args
    if is_json(args):
        return json.loads(args)
    elif isinstance(args, dict):
        return args
    else:
        raise Exception(
            'Invalid Arg!'
        )


def env_abbrv_to_wordlike(which: str = None) -> str:
    return dict(dv='devl', qa='qual', pr='prod')[
        ENV if not which else which
    ]


def error_handles(kind: tuple, mssg: str) -> callable:
    def _error_handles(func):
        @wraps(func)
        def inner(*args, **kwargs):
            try:
                return func(*args, **kwargs)
            except kind:
                raise Exception(mssg)
        return inner
    return _error_handles


def home_dir():
    return expanduser('~')


def is_json(value: str) -> bool:
    try:
        json.loads(value)
    except (ValueError, TypeError):
        return False
    return True


def load_dotenv_vars(path: str = None) -> None:
    if not path:  path = PATH_SECRETS
    for root, dirs, files in os.walk(path):
        for file in files:
            if file.endswith('.env'):
                load_dotenv(
                    f'{path}/{file}'
                )


def local_args(_locals: dict) -> tuple:
    return tuple(_locals.values())


def local_kwargs(_locals: dict) -> dict:
    return dict(zip(_locals.keys(), _locals.values()))


def run(pipelines: iter) -> None:
    procs = dict()
    for n, q in enumerate(pipelines):
        if not isinstance(q.args, tuple):
            raise Exception('Args must be packaged in a tuple!')
        kwargs = dict(target=q.func, args=q.args)
        p = mp.Process(**kwargs)
        p.daemon = True
        p.start()
        procs[n] = dict(
            process=p,
            kwargs=kwargs
        )
    while True:
        for n, pckg in procs.items():
            p, inkwargs = pckg['process'], pckg['kwargs']
            if not p.is_alive():
                LOGGER.info(f'Restarted {n}: {inkwargs}')
                p.terminate()
                sleep(0.05)
                p = mp.Process(**inkwargs)
                p.daemon = True
                p.start()
                procs[n] = dict(
                    process=p,
                    kwargs=inkwargs
                )


def warn_handles(kind: tuple, mssg: str) -> callable:
    def _warn_handles(func):
        @wraps(func)
        def inner(*args, **kwargs):
            try:
                return func(*args, **kwargs)
            except kind:
                LOGGER.warning(mssg)
                pass
        return inner
    return _warn_handles
