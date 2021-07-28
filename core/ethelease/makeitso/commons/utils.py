#!/usr/bin/env python

import yaml
from ethelease.commons.utils import home_dir


def grab_inits() -> yaml:
    mssg = '`ethelease` NOT initialized!'
    try:
        with open(f'{home_dir()}/.ethel') as fin:
            return yaml \
                .safe_load(
                    fin
                )
    except:
        raise Exception(mssg)
