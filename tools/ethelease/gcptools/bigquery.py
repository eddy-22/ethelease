#!/usr/bin/env python

import os
from collections import namedtuple
from time import sleep

from google.auth import default
from google.api_core.exceptions import BadRequest
from google.cloud import bigquery
from google.cloud.bigquery import Client, ExtractJobConfig
from google.cloud.exceptions import NotFound
from ethelease.commons.utils import ENV, LOGGER


BQLoadConfig = namedtuple('BQLoadConfig', ('project', 'dataset', 'table'))
PT = '_PARTITIONTIME'


if os.environ.get('GOOGLE_APPLICATION_CREDENTIALS'):
    CREDS, _ = default(
        scopes=[
            'https://www.googleapis.com/auth/drive',
            'https://www.googleapis.com/auth/bigquery',
        ]
    )
else:
    CREDS = None


def bq_client(project_id=None):
    if CREDS and project_id:
        return Client(
            project=project_id,
            credentials=CREDS
        )


def does_dataset_exist(project_id: str, dataset_id: str):
    try:
        bq_client(project_id).get_dataset(dataset_id)
        return True
    except NotFound:
        return False


def does_table_exist(project_id: str, dataset_id: str, table_id: str):
    try:
        bq_client(project_id).get_table(f'{project_id}.{dataset_id}.{table_id}')
        return True
    except NotFound:
        return False


def drop_create_bq_table(project_id, dataset_id, table_id, schema):
    """ This function is TO BE only used in limited circumstances """
    bq_client(project_id).delete_table(table_id, not_found_ok=True)
    sleep(1)
    table = bigquery.Table(f'{project_id}.{dataset_id}.{table_id}', schema)
    table.time_partitioning = bigquery.TimePartitioning(type_=bigquery.TimePartitioningType.DAY)
    bq_client(project_id).create_table(table)


def drop_view(project_id, dataset_id, view_id):
    bq_client(project_id).delete_table(
        f'{project_id}.{dataset_id}.{view_id}',
        not_found_ok=True
    )


def extract_bq_table(project_id, dataset_id, table_id, partition, destination, frmt):

    def dest_format(kind):
        bq_client_dest_frmt = bigquery.DestinationFormat
        dest_frmt = dict(
            CSV=bq_client_dest_frmt.CSV,
            JSON=bq_client_dest_frmt.NEWLINE_DELIMITED_JSON,
            AVRO=bq_client_dest_frmt.AVRO
        )
        return dest_frmt.get(kind)

    extract_conf = ExtractJobConfig()
    extract_conf.destination_format = dest_format(frmt)
    if frmt == 'CSV': extract_conf.field_delimiter = '\t'
    table_str = f'{table_id}${partition}' if partition else f'{table_id}'
    table_ref = bq_client(project_id).dataset(dataset_id=dataset_id).table(table_str)
    bq_client(project_id).extract_table(
        table_ref,
        destination,
        job_config=extract_conf
    )


def grab_bq_table_date_partitions(project_id, dataset_id, table_id):
    query = f'SELECT DISTINCT DATE(_PARTITIONTIME) AS pt FROM `{project_id}.{dataset_id}.{table_id}`'
    result = bq_client(project_id).query(query)
    for pt in result:
        yield pt


def grab_bq_client_table_schema(project_id, dataset_id, table_id):
    table = bq_client(project_id).get_table(f'{project_id}.{dataset_id}.{table_id}')
    return table.schema


def grab_bq_client_table_size_GBs(project_id, dataset_id, table_id):
    table = bq_client(project_id).get_table(f'{project_id}.{dataset_id}.{table_id}')
    return table.num_bytes / pow(10, 9)


def make_bq_dataset_if_needed(project_id=None, dataset_id=None):
    exists = list(ds.dataset_id for ds in bq_client(project_id).list_datasets() if ds.dataset_id == dataset_id)
    if not exists:
        dataset = bigquery.Dataset(f'{project_id}.{dataset_id}')
        dataset.location = 'US'  # us-central1 not publicly available yet so said the error
        bq_client(project_id).create_dataset(dataset)
        LOGGER.info(f'{dataset_id} created!')


def make_bq_table_if_needed(project_id: str, dataset_id: str, table_id: str, schema: str, pt_key: str, pt_type: str):
    exists = list(t.table_id for t in bq_client(project_id).list_tables(dataset_id) if t.table_id == table_id)
    if not exists:
        table = bigquery.Table(
            f'{project_id}.{dataset_id}.{table_id}',
            schema=schema
        )
        if pt_key:
            if pt_key == PT:
                table.time_partitioning = bigquery \
                    .TimePartitioning(type_='DAY' if not pt_type else pt_type)
            elif pt_key != PT:
                kwarg = dict(field=pt_key)
                table.time_partitioning = bigquery \
                    .TimePartitioning(**kwarg)
        bq_client(project_id).create_table(table)
        LOGGER.info(f'{table_id} created!')


def make_bq_view_if_needed(project_id=None, dataset_id=None, view_id=None, view_query=None):
    exists = list(t.table_id for t in bq_client(project_id).list_tables(dataset_id) if t.table_id == view_id)
    if not exists:
        view = bigquery.Table(f'{project_id}.{dataset_id}.{view_id}')
        view.view_query = view_query
        bq_client(project_id).create_table(view)
        LOGGER.info(f'{view_id} created!')


def stream_data_to_bq(data: list, client: bigquery.Client, project_id: str, dataset_id: str, table_id: str):
    table = client.get_table(f'{project_id}.{dataset_id}.{table_id}')
    client.insert_rows(table, data)


class BQTableExistence:

    __slots__ = ['_dataset', '_project', '_table', '_timeout', 'i']

    def __init__(self):
        self._dataset = None
        self._project = None
        self._table = None
        self._timeout = 300
        self.i = 0

    def exists(self):
        while self.i <= self._timeout:
            if does_dataset_exist(self._project, self._dataset):
                tables = list(
                    t.table_id
                    for t in BQ(self._project).list_tables(self._dataset)
                    if t.table_id == self._table
                )
                if tables:
                    return True
            else:
                return False
            if self._timeout == self.i:
                LOGGER.info(f'Time out!  Table {self._table} doesn\'t exist!')
                return False
            sleep(1)
            self.i += 1
        return False

    def bq_config(self, conf: BQLoadConfig):
        self._project, self._dataset, table_id = conf
        if table_id.find('$') != -1:
            self._table = table_id.split('$')[0]
        else:
            self._table = table_id
        return self

    def time_to_timeout(self, timeout):
        self._timeout = timeout
        return self

    # Legacy
    def project_id(self, project):
        self._project = project
        return self

    # Legacy
    def dataset_id(self, dataset):
        self._dataset = dataset
        return self

    # Legacy
    def table_id(self, table):
        self._table = table
        return self


class LoadToBQFromGCS:

    __slots__ = ['_bq_client', '_dataset_ref', '_conf', '_table', '_source_formats']

    def __init__(self):
        self._bq_client, self._dataset_ref, self._conf, self._table = None, None, None, None
        self._source_formats = dict(
            CSV=bigquery.SourceFormat.CSV,
            JSON=bigquery.SourceFormat.NEWLINE_DELIMITED_JSON,
            PARQUET=bigquery.SourceFormat.PARQUET
        )

    def make_config(self, project_id=None, dataset_id=None, table_id=None):
        self._bq_client = bq_client(project_id)
        self._table = self._bq_client.get_table(f'{project_id}.{dataset_id}.{table_id}')
        self._conf = bigquery.LoadJobConfig()
        return self

    def write_truncate(self, state):
        check = isinstance(state, bool)
        if not check: raise Exception('Type must be Boolean!')
        if check and state is True:
            self._conf.write_disposition = (
                bigquery
                    .WriteDisposition
                    .WRITE_TRUNCATE
            )
        return self

    def source_format(self, kind=None, delimiter=None, skip_header=False):
        if kind not in self._source_formats:
            raise Exception('Bad format!')
        if kind == 'CSV' and delimiter:
            self._conf.field_delimiter = delimiter
        if kind == 'CSV' and not delimiter:
            self._conf.field_delimiter = '\t'
        if kind == 'CSV' and skip_header:
            self._conf.skip_leading_rows = 1
        self._conf.source_format = (
            self._source_formats
                .get(kind)
        )
        return self

    def schema(self, schema):
        if schema is not None:
            self._conf.schema = schema
        if schema is None:
            self._conf.autodetect = True
        return self

    def load_source(self, src_uri):
        job = self._bq_client.load_table_from_uri(
            src_uri,
            self._table,
            job_config=self._conf
        )
        LOGGER.info(f'Loaded to {self._table}!')
        if ENV in {'dv', 'qa', }:
            for j in self._bq_client.list_jobs(state_filter='RUNNING'):
                LOGGER.info(j.job_id)
            try:
                job.result()
            except BadRequest:
                LOGGER.info('Check error stream!')
                raise
        return self
