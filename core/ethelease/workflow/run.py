#!/usr/bin/env python

import multiprocessing as mp
import os
import yaml
from collections import namedtuple
from time import sleep
from ethelease.commons.utils import ENV, LOGGER
from ethelease.cronos.utils import eval_cron
from ethelease.k8s.ops import pod_launch_n_mgmt, K8sPodConf
from ethelease.workflow.commons.utils import Valuables, scheds, scheduler


FAMILY = os.environ.get('_FAMILY_')


def sleeper() -> dict:
    groups, _index = list(zip(*[iter(range(1, 101))]*10)), dict()
    for j, group in enumerate(groups):
        for x in group:
            _index[x] = j
    return _index


def workflow(pipeline_name: str, vals: Valuables) -> None:
    kind, sched = vals.kind, vals.schedule
    mssg, devil = f'`{pipeline_name}` launching in `{ENV}`!', {'develop', 'fix'}
    conf_body = (
        K8sPodConf()
            .env(ENV)
            .family_name(vals.name)
            .container_registry(vals.registry_location)
            .metadata(name=pipeline_name, proj_or_acct_id= vals.proj_or_acct_id, namespace='default')
            .which_nodepoolorgroup(name=vals.node_poolorgroup, cloud=vals.which_cloud)
            .pipeline_or_runner_script(vals.script)
            .script_args(vals.args)
            .pick_secret(name=vals.which_secret)
            .cpu_usage(
                req=vals.req_cpu,
                lim=vals.lim_cpu
            )
            .mem_usage(
                req=vals.req_mem,
                lim=vals.lim_mem
            )
            .restart_policy(vals.restart_policy)
            .assemble()
    )
    if ENV == 'pr' and sched != 'immediately_once':
        while True:
            if eval_cron(pipeline_name=pipeline_name, expr=sched):
                LOGGER.info(mssg)
                pod_launch_n_mgmt(conf_body)
    if ENV == 'pr' and sched == 'immediately_once':
        LOGGER.info(mssg)
        pod_launch_n_mgmt(conf_body)
        while True:
            sleep(60)
    if ENV == 'dv' and sched in devil:
        LOGGER.info(mssg)
        pod_launch_n_mgmt(conf_body)
        while True:
            sleep(60)
    if ENV == 'dv' and sched != devil:
        while True:
            LOGGER.info('Nothing here...')
            sleep(300)


if __name__ == '__main__':

    scheduler(
        scheds(
            '.',
            family=os.environ.get('_FAMILY_')
        ),
        workflow
    )
