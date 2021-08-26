#!/usr/bin/env python

'''

MOST LIKELY DEPRECATED!!! -Will aka WillemRvX

'''


import json
from psycopg2.extras import DictCursor
from ethelease.commons.etl_tools import existence_of_all, file_name
from ethelease.gcptools.bigquery import (
    BQLoadConfig,
    LoadToBQFromGCS,
    make_bq_dataset_if_needed,
    make_bq_table_if_needed
)
from ethelease.gcptools.cloudstorage import gcs_objects_list


class ETL:

    kind, writable, timeout, batchsize, bucket = None, None, 300, 500000, None

    def set_timeout(self, n: int):
        self.timeout = n
        return self

    def batch_size(self, n):
        self.batchsize = n
        return self

    def writer(self, w: callable, bucket: str, blob: str):
        self.bucket = bucket
        self.writable = w(bucket, blob)
        return self

    def output_file_type(self, kind: str):
        if kind in {'csv', 'json'}:
            self.kind = kind
        else:
            raise Exception(
                'Improper file kind!'
            )
        return self

    def _exists(self, blobs: list):
        return existence_of_all(
            bucket=self.bucket,
            blobs=blobs,
            look_up=gcs_objects_list,
            timeout_time=self.timeout
        )


class ExtractorFromRDBMS(ETL):

    _db_kind, conn, table = None, None, None

    def db_kind(self, kind):
        if kind in {'mysql', 'oracle', 'postgres'}:
            self._db_kind = kind
        else:
            raise Exception('Not a good RDBMS!')
        return self

    def connection(self, conn):
        self.conn = conn
        return self

    def which_table(self, table: str):
        self.table = table
        return self

    def _check_if_something(self, conn):
        inner_query = f'SELECT * FROM {self.table}'
        inner_query += ' LIMIT 1' if self._db_kind != 'oracle' else ' WHERE ROWNUM <= 1'
        inner_curse = conn.cursor()
        inner_curse.execute(inner_query)
        fetch_one = inner_curse.fetchone()
        inner_curse.close()
        if fetch_one:
            return True
        return False

    def _cursor(self, conn):
        kwargs = dict(
            postgres=dict(cursor_factory=DictCursor),
            mysql=dict(dicionary=True),
            oracle=None
        )
        cur = conn.cursor(**kwargs[self._db_kind])
        if self._db_kind in {'mysql', 'postgres'}:
            cur.itersize = self.batchsize
        cur.arraysize = self.batchsize
        return cur

    def proc(self, proc_col: callable, query: str):
        with self.conn as conn:
            blobs, self.kind = list(), 'json'
            if self._check_if_something(conn):
                curse, data, i = self._cursor(conn), list(), 0
                curse.execute(query)
                fetcher = curse.fetchall() if self._db_kind == 'oracle' else curse
                for j, row in enumerate(fetcher, 1):
                    row = json.dumps({k: proc_col(v) for k, v in row.items()})
                    data.append(row)
                    if j % self.batchsize == 0:
                        data = '\n'.join(data)
                        blob = self.writable(data, file_name(self.kind))
                        blobs.append(blob)
                        i += 1
                        data = list()
                if data:
                    data = '\n'.join(data)
                    blob = self.writable(data, file_name(self.kind))
                    blobs.append(blob)
            curse.close()
            if blobs:
                return (
                    self._exists(blobs)
                )


class Transformer(ETL):

    source = None

    def data_src(self, source: iter):
        self.source = source
        return self

    def proc(self, func: callable):
        data, blobs = list(), list()
        for i, r in enumerate(self.source, 1):
            data.append(func(r))
            if i % self.batchsize == 0:
                blob = self.writable('\n'.join(data), file_name(self.kind))
                blobs.append(blob)
                data = list()
        if data:
            blob = self.writable(
                '\n'.join(data),
                file_name(self.kind)
            )
            blobs.append(blob)
        if blobs:
            return (
                self._exists(blobs)
            )


class LoaderBQ:

    confs, project, dataset, table, schema, appendable, format = None, None, None, None, None, False, None
    pt_type, pt_key, path = None, None, None

    def make_configs(self, confs: BQLoadConfig):
        self.confs = confs
        self.project = confs.project
        self.dataset = confs.dataset
        self.table = confs.table
        return self

    def partition_spec(self, kind: str, key: str):
        self.pt_type, self.pt_key = kind, key
        return self

    def provide_schema(self, schema: list):
        self.schema = schema
        return self

    def which_source_format(self, frmt: dict):
        self.format = frmt
        return self

    def append(self, state: bool):
        self.appendable = state
        return self

    def from_where(self, path: str):
        self.path = path
        return self

    def _kwargs(self):
        return dict(
            dataset_id=self.dataset,
            table_id=self.table.split('$')[0],
            schema=self.schema,
            pt_key=self.pt_key,
            pt_type=self.pt_type
        )

    def proc(self):
        make_bq_dataset_if_needed(self.project, self.dataset)
        make_bq_table_if_needed(self.project, **self._kwargs())
        (
            LoadToBQFromGCS()
                .make_config(*self.confs)
                .write_truncate(not self.appendable)
                .source_format(**self.format)
                .schema(self.schema)
                .load_source(self.path)
        )
