#!/usr/bin/env python

import os
import signal
from subprocess import Popen, PIPE
from contextlib import contextmanager
from time import sleep
from ethelease.commons.utils import local_args


@contextmanager
def CloudSQLProxy(instance_name: str, port: str, creds: str) -> None:

    def sql_proxy_comm():
        return ' '.join([
            './cloud_sql_proxy',
            f'-instances={instance_name}=tcp:{port}',
            f'-credential_file={creds}',
            '&'
        ])

    proxy = Popen(
        sql_proxy_comm().split(),
        stdout=PIPE,
        preexec_fn=os.setsid)
    sleep(10)
    try:
        yield proxy
    finally:
        os.killpg(
            os.getpgid(proxy.pid),
            signal.SIGTERM
        )
