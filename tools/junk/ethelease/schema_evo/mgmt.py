#!/usr/bin/python


'''

MOST LIKELY DEPRECATED!!! -Will aka WillemRvX

'''


import io
import json
from collections import namedtuple
from google.cloud.bigquery.schema import SchemaField
from pymysql.cursors import DictCursor
from gcptools.ethelease.gcptools.bigquery import bq_client, BQLoadConfig
from gcptools.ethelease.gcptools.cloudstorage import read_list_from_gcs, write_file_to_gcs


BQ = bq_client()
PkeyAndType = namedtuple('PkeyAndType', ('pkey', 'pk_type'))


# Not complete...
PSQL_TO_BQ_DTYPE_MAP = {
    'ARRAY': 'STRING',
    'bigint': 'INT64',
    'boolean': 'BOOL',
    'character varying': 'STRING',
    'date': 'DATE',
    'double precision': 'FLOAT64',
    'float': 'FLOAT64',
    'integer': 'INT64',
    'jsonb': 'STRING',
    'numeric': 'NUMERIC',
    'smallint': 'INT64',
    'timestamp with time zone': 'TIMESTAMP',
    'timestamp without time zone': 'TIMESTAMP',
    'USER-DEFINED': 'STRING',
    'uuid': 'STRING',
    'text': 'STRING',
}

MYSQL_TO_BQ_DTYPE_MAP = {
    'bigint': 'INT64',
    'date': 'DATE',
    'datetime': 'DATETIME',
    'decimal': 'NUMERIC',
    'float': 'FLOAT64',
    'int': 'INT64',
    'longtext': 'STRING',
    'text': 'STRING',
    'time': 'TIME',
    'timestamp': 'TIMESTAMP',
    'tinyint': 'INT64',
    'varchar': 'STRING',
}


def bq_schema_evo_check(tbl_exists: bool, conf: BQLoadConfig, src_schema: list):
    if tbl_exists:
        if conf.table.find('$') != -1:
            _table = conf.table.split('$')[0]
        else:
            _table = conf.table
        new_schema = bq_schema_evo(
            BQ(conf.project),
            f'{conf.project}.{conf.dataset}.{_table}',
            src_schema
        )
        if new_schema:
            return new_schema
        else:
            return src_schema
    return src_schema


def bq_schema_evo(bqclient, tbl_str: str, schema: list):

    def new_fields(og_schema: list, schema: list):
        return list(s for s in schema if s.name not in set(s.name for s in og_schema))

    result, catch = None, 'Updated description.'
    og_table = bqclient.get_table(tbl_str)
    og_schema = og_table.schema
    new_cols = new_fields(og_schema, schema)
    new_schema = og_schema[:]

    if new_cols:
        new_schema.extend(new_cols)
        og_table.schema = new_schema
        result = bqclient \
            .update_table(
            og_table,
            ['schema']
        )
        if result.description == catch:
            return new_schema


def bq_schema_to_json(schema):
    list_of_fields = list()
    for col in schema:
        if isinstance(col, SchemaField):
            list_of_fields.append(
                dict(
                    description=col.description,
                    mode=col.mode,
                    name=col.name,
                    field_type=col.field_type
                )
            )
    return json.dumps(list_of_fields)


# DEPRECATED
def grab_primary_key(Conn, table):
    """ https://wiki.postgresql.org/wiki/Retrieve_primary_key_columns """

    def convert(pk_type):
        if pk_type.find('character varying') != -1:
            return 'varchar'
        return 'integer'

    with Conn as conn:
        curse = conn.cursor()
        curse.execute(
            'SELECT a.attname, format_type(a.atttypid, a.atttypmod) AS data_type '
            'FROM pg_index i '
            'JOIN pg_attribute a '
            'ON a.attrelid = i.indrelid '
            'AND a.attnum = ANY(i.indkey) '
            f'WHERE i.indrelid = \'{table}\'::regclass '
            'AND i.indisprimary '
        )
        primary_key = list(
            PkeyAndType(c[0], convert(c[1]))
            for c in curse.fetchall()
        )
        if len(primary_key) == 1:
            return primary_key[0]


def grab_mysql_primary_key(Conn, table):
    with Conn as conn:
        curse = conn.cursor(DictCursor)
        query = f'SHOW INDEXES FROM {table} WHERE Key_name = \'PRIMARY\''
        curse.execute(query)
        pkey = curse.fetchall()
        curse.close()
        return list(k['Column_name'] for k in pkey)


# Not tested yet...
def grab_psql_primary_key(Conn, table):
    """ https://wiki.postgresql.org/wiki/Retrieve_primary_key_columns """

    def convert(pk_type):
        if pk_type.find('character varying') != -1:
            return 'varchar'
        return 'integer'

    with Conn as conn:
        curse = conn.cursor()
        curse.execute(
            'SELECT a.attname, format_type(a.atttypid, a.atttypmod) AS data_type '
            'FROM pg_index i '
            'JOIN pg_attribute a '
            'ON a.attrelid = i.indrelid '
            'AND a.attnum = ANY(i.indkey) '
            f'WHERE i.indrelid = \'{table}\'::regclass '
            'AND i.indisprimary '
        )
        primary_key = list(
            PkeyAndType(c[0], convert(c[1]))
            for c in curse.fetchall()
        )
        return primary_key


def grab_mysql_schema(Conn, table):
    with Conn as conn:
        curse = conn.cursor()
        curse.execute(f'DESCRIBE {table}')
        return list(
            dict(name=c[0], datatype=c[1].split('(')[0]) for c
            in curse.fetchall()
        )


def grab_psql_schema(Conn, table):
    with Conn as conn:
        curse = conn.cursor()
        curse.execute(
            'SELECT column_name, data_type FROM information_schema.columns '
            f'WHERE table_name = \'{table}\''
            'ORDER BY ordinal_position'
        )
        return list(
            dict(name=c[0], datatype=c[1]) for c
            in curse.fetchall()
        )


def mysql_to_bq_schema(mysql_schema):
    return _convert_to_bq_schema(
        mysql_schema,
        MYSQL_TO_BQ_DTYPE_MAP
    )


def psql_to_bq_schema(psql_schema):
    return _convert_to_bq_schema(
        psql_schema,
        PSQL_TO_BQ_DTYPE_MAP
    )


def _convert_to_bq_schema(schema, mapping):
    return list(
        SchemaField(
            **dict(
                name=f['name'],
                field_type=mapping[f['datatype']],
                mode='Nullable'
            )
        )
        for f in schema
    )


def to_bq_schema(schema):
    return list(
        SchemaField(**sf) for sf in schema
    )


def extract_rdms_schema_to_gcs(bq_schema, bucket, as_blob):
    bq_schema = json.loads(bq_schema_to_json(bq_schema))
    bq_schema = json.dumps(bq_schema)
    write_file_to_gcs(
        bucket=bucket,
        blob=as_blob,
        file_obj=io.StringIO(bq_schema)
    )


def grab_bq_ready_schema_from_gcs(datalake, schema_path_blob):
    bq_ready_schema = read_list_from_gcs(
        bucket_name=datalake,
        blob_name=schema_path_blob
    )
    return to_bq_schema(bq_ready_schema)


def grab_mysql_schema_as_bq_ready(Conn, table):
    mysql_schema = grab_mysql_schema(Conn, table)
    bq_schema = mysql_to_bq_schema(mysql_schema)
    return bq_schema


def grab_psql_schema_as_bq_ready(Conn, table):
    psql_schema = grab_psql_schema(Conn, table)
    bq_schema = psql_to_bq_schema(psql_schema)
    return bq_schema
