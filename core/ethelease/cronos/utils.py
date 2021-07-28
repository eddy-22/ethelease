#!/usr/bin/env python

from datetime import datetime
from time import sleep
import pytz
from tzlocal import get_localzone
from croniter import croniter
from ethelease.commons.utils import LOGGER


def eval_cron(pipeline_name: str, expr: str) -> None:
    utc_dt = pytz.timezone(str(get_localzone())).localize(datetime.utcnow())
    LOGGER.info(f'`{pipeline_name}` next run at {croniter(expr, utc_dt).get_next(datetime)}')
    while True:
        cron_iter, now = croniter(expr, utc_dt), datetime.utcnow()
        nc = next(cron_iter)
        if nc - now.timestamp() <= 0:
            return
        sleep(0.5)
