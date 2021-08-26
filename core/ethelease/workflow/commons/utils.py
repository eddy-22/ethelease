#!/usr/bin/env python

import multiprocessing as mp
import os
import yaml
from collections import namedtuple
from time import sleep
from ethelease.commons.utils import ENV, LOGGER
from ethelease.cronos.utils import eval_cron
from ethelease.k8s.ops import pod_launch_n_mgmt, K8sPodConf


MultiProcArg = namedtuple(
    'MultiProcArg',
    (
        'pipeline_name',
        'valuables'
    )
)

Valuables = namedtuple(
    'Valuables',
    (
        'name',
        'kind',
        'schedule',
        'script',
        'args',
        'proj_or_acct_id',
        'registry_location',
        'which_cloud',
        'node_poolorgroup',
        'lim_cpu',
        'lim_mem',
        'req_cpu',
        'req_mem',
        'restart_policy',
        'which_secret',
    )
)


def scheds(loc: str, family: str) -> dict:

    def serargs(args: dict) -> json:
        if not isinstance(args, dict):
            raise Exception('Args must by a YAML map / dict!')
        return json.dumps(args)

    with open(f'{loc}/{family}/configs/inits.yaml') as inits:
        init_pool = yaml.safe_load(inits)
        which_cloud, registry_loc = init_pool['cloud'], init_pool['registry']
        if which_cloud == 'gcp':
            proj_or_acct_id = init_pool['gcp_project_id']
        with open(f'{loc}/{family}/configs/specs.yaml') as confs:
            river_o_confs = yaml.safe_load(confs)
            river_o_confs.pop('scheduler')
            return {
                pipeline_name: Valuables(
                    family,
                    conf['Kind'],
                    conf['Schedule'],
                    conf.get('PipelineScript', conf.get('RunnerScript')),
                    serargs(conf['Args']),
                    proj_or_acct_id,
                    registry_loc,
                    which_cloud,
                    conf.get('NodePoolOrGroup'),
                    conf['LimCpu'],
                    conf['LimMem'],
                    conf['ReqCpu'],
                    conf['ReqMem'],
                    conf.get('RestartPolicy', 'Never'),
                    conf.get('WhichSecret')
                )
                for pipeline_name, conf
                in river_o_confs.items()
            }


def scheduler(scheds: dict, workflow: callable, is_local: bool) -> None:
    procs, limit = dict(), dict(dv=3, pr=1000)
    for n, (p, v) in enumerate(scheds.items()):
        kwargs = dict(target=workflow, args=MultiProcArg(p, v))
        p = mp.Process(**kwargs)
        if not is_local:
            p.daemon = True
            procs[n] = dict(
                process=p,
                kwargs=kwargs,
                restarts=0
            )
        p.start()
    if not is_local:
        while True:
            for n, pckg in procs.items():
                p, inkwargs = pckg['process'], pckg['kwargs']
                if not p.is_alive():
                    pipeline_name = inkwargs['args'].pipeline_name
                    LOGGER.info(f'Restarting: {pipeline_name}')
                    p.terminate()
                    sleep(0.05)
                    p = mp.Process(**inkwargs)
                    p.daemon = True
                    p.start()
                    procs[n] = dict(
                        process=p,
                        kwargs=inkwargs,
                        restarts=procs[n]['restarts']+1
                    )
                if procs[n]['restarts'] > limit[ENV]:
                    raise Exception(
                        'Something\'s wrong...'
                    )
