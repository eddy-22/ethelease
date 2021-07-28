#!/usr/bin/env python

import json
from ethelease.commons.utils import env_abbrv_to_wordlike, is_json


def test_env_abbrv_to_wordlike() -> None:
    test = env_abbrv_to_wordlike
    _dv, _qa, _pr = test(which='dv'), test(which='qa'), test(which='pr')
    assert _dv == 'devl'
    assert _qa == 'qual'
    assert _pr == 'prod'


def test_is_json() -> None:
    nay = is_json('1b2b3')
    yay = is_json(json.dumps({'1b2b3': [0, 1, 2]}))
    assert nay is False
    assert yay is True
