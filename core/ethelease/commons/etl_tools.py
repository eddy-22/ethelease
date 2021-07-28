#!/usr/bin/env python

from collections import namedtuple
from datetime import date, datetime, timedelta
from time import sleep
from uuid import uuid4
from ethelease.commons.utils import LOGGER


DatetimeStamps = namedtuple('DateStamper', ('ds', 'dt', 'dth', 'hr', 'td_mdn', 'ys_mdn', 'lag1_ds', 'lag1_hr', 'lhr'))
FileName = namedtuple('FileName', ('name', 'uuid'))
TimeTag = namedtuple('TimeTag', ('time', 'uuid'))


def datetime_formats(kind: str) -> str:
    ds_frmt = '%Y-%m-%d'
    return dict(
        hour_only=f'{ds_frmt} %H:00:00',
        iso8601=ds_frmt,
        nanoseconds=f'{ds_frmt} %H:%M:%S.%f',
        seconds=f'{ds_frmt} %H:%M:%S'
    )[kind]


def datetimestamps() -> DatetimeStamps:
    now = datetime.utcnow()
    back1_dy, back1_hr = timedelta(days=1), timedelta(hours=1)
    today_midnight = datetime.combine(now.date(), datetime.min.time())
    yesterday_midnight = today_midnight - back1_dy
    lag1_hr = (now - back1_hr)
    iso_8601_frmt, hr_only_frmt, = datetime_formats('iso8601'), datetime_formats('hour_only')
    return DatetimeStamps(
        now.strftime(iso_8601_frmt),
        now.strftime(datetime_formats('nanoseconds')),
        now.strftime(hr_only_frmt),
        str(now.hour).zfill(2),
        today_midnight,
        yesterday_midnight,
        (now - back1_dy).strftime(iso_8601_frmt),
        lag1_hr.strftime(hr_only_frmt),
        str(lag1_hr.hour).zfill(2)
    )


def daterange(start_date: date, end_date: date) -> date:
    for n in range(int((end_date - start_date).days)):
        yield start_date + timedelta(n)


def existence_of_all(bucket: str, blobs: list, look_up: callable, timeout_time: int) -> bool:
    if blobs:
        states = list()
        for blob in blobs:
            states.append(
                Existence()
                    .bucket(bucket)
                    .key(blob)
                    .time_to_timeout(timeout=timeout_time)
                    .lookup(look_up)
                    .exists()
            )
        return all(states)
    else:
        return False


def file_name(ext: str) -> str:
    time, uuid = time_tag()
    return f'data-{time}-{uuid}.{ext}'


def time_tag() -> TimeTag:
    return TimeTag(
        str(datetime.utcnow().time()).split('.')[0].replace(':', ''),
        str(uuid4()).replace('-', '')[0:13]
    )


class Existence:

    __slots__ = ['_bucket', 'i', '_key', '_lookup', '_timeout']

    def __init__(self):
        self._bucket = None
        self.i = 0
        self._key = None
        self._lookup = None
        self._timeout = 300

    def exists(self):
        while self.i <= self._timeout:
            if self._timeout == self.i:
                LOGGER.info('Time out!')
                return False
            elif self._lookup(self._bucket, self._key):
                return True
            sleep(1)
            self.i += 1
        return False

    def bucket(self, bucket):
        self._bucket = bucket
        return self

    def key(self, key):
        self._key = key
        return self

    def lookup(self, func: callable):
        self._lookup = func
        return self

    def time_to_timeout(self, timeout):
        self._timeout = timeout
        return self


class PathBuilder:

    __slots__ = ['_add_star',
                 '_bucket',
                 '_env',
                 '_source',
                 '_sub_source',
                 '_ds',
                 '_kind',
                 '_file',
                 '_pop_file', ]

    def __init__(self):
        self._add_star = False
        self._bucket = None
        self._env = None
        self._source = None
        self._sub_source = None
        self._ds = None
        self._kind = None
        self._file = None
        self._pop_file = False

    def blob(self):
        assembly = self._blob_parts_list()
        if self._pop_file:
            assembly[5] = None
        if self._add_star:
            assembly.append('*')
        return '/'.join(list(part for part in assembly if part))

    def _blob_parts_list(self):
        return [self._env,
                self._source,
                self._sub_source,
                self._ds,
                self._kind,
                self._file]

    def bucket(self, name: str, cloud: str):
        if cloud not in {'aws', 'gcp', }: raise Exception('Cloud not supported.')
        if name.find(':') > -1: raise Exception('Not needed!')
        prfx = dict(aws='s3', gcp='gs',)
        self._bucket = f'{prfx[cloud]}://{name}'
        return self

    def full_path(self):
        return '/'.join(
            [self._bucket, self.blob()]
        )

    def pop_file(self, makepop: bool):
        self._pop_file = makepop
        return self

    def append_asterisk(self, addstar: bool):
        self._add_star = addstar
        return self

    def env(self, env):
        if env not in {'dv', 'qa', 'pr', }:
            raise Exception('Not a legit environment!')
        self._env = f'env={env}'
        return self

    def source(self, source):
        self._source = f'source={source}'
        return self

    def subsource(self, sub_source):
        self._sub_source = f'subsrc={sub_source}'
        return self

    def kind(self, kind):
        if kind not in {'backfill', 'metadata', 'raw', 'schema', 'transformed', }:
            raise Exception('Not an acceptable kind!')
        self._kind = f'kind={kind}'
        return self

    def ds(self, ds: str):
        """ Enter ISO 8601 compliant datestamp! """
        _ds = ds.split('-')
        if len(_ds) != 3:
            raise Exception('Not a valid ISO 8601 datestamp!')
        try:
            datetime.fromisoformat(ds)
        except ValueError:
            raise
        yr, mo, dy = _ds
        self._ds = f'yr={yr}/mo={mo}/dy={dy}'
        return self

    def file_name(self, name: str):
        self._file = name
        return self
