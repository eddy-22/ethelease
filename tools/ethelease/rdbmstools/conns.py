#!/usr/bin/env python

from contextlib import contextmanager
import psycopg2
from mysql.connector import connect
from psycopg2.extras import LogicalReplicationConnection
from ethelease.commons.utils import local_kwargs


# The psycopg2 connection object can  already  be
# used as a context manager.  But will leave this
# one here as it allows for more customization


@contextmanager
def MySQLConn(user, password, host, database, port):
    kwargs = local_kwargs(locals())
    kwargs.update(dict(charset='utf8', ))
    conn = connect(**kwargs)
    try:
        yield conn
    finally:
        conn.close()


@contextmanager
def PsqlConn(dbname, user, password, host, port):
    kwargs = local_kwargs(locals())
    kwargs.update(dict(sslmode='require'))
    conn = psycopg2.connect(**kwargs)
    try:
        yield conn
    finally:
        conn.close()


@contextmanager
def PsqlConnNoSSL(dbname, user, password, host, port):
    kwargs = local_kwargs(locals())
    kwargs.update(dict(sslmode='disable'))
    conn = psycopg2.connect(**kwargs)
    try:
        yield conn
    finally:
        conn.close()


@contextmanager
def PsqlReplicaConn(dbname, user, password, host, port):
    kwargs = local_kwargs(locals())
    kwargs.update(dict(connection_factory=LogicalReplicationConnection))
    conn = psycopg2.connect(**kwargs)
    try:
        yield conn
    finally:
        conn.close()
