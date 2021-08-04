#!/usr/bin/env python

import json
import sys
from json.decoder import JSONDecodeError

import docker
from docker import APIClient
from docker.errors import APIError
from ethelease.commons.utils import ENV, home_dir, LOGGER
from ethelease.k8s.localops import is_kubectl_installed, launch_pod_on_local, pod_status_local
from ethelease.k8s.ops import K8sPodConf
from ethelease.makeitso.commons.utils import grab_inits
from ethelease.makeitso.puncher import project_location
from ethelease.workflow.commons.utils import Valuables, scheds, scheduler


def local_docker_build(proj_name: str, member_name: str) -> None:

    inits = grab_inits()
    registry, proj_or_acct_id =  inits['registry'], inits.get('gcp_project_id')
    proj_loc = project_location(proj_name, inits['local_repo_dir'])
    client, loc_tag = docker.from_env(), f'dv-{proj_name}-{member_name}'
    tag = f'{registry}/{proj_or_acct_id}/{loc_tag}'

    def is_docker_installed() -> None:
        try:
            _path = '.docker/config.json'
            with open(f'{home_dir()}/{_path}') as conf:
                conf = json.load(conf)
                if conf:
                    print('`Docker` found!  Can proceed...')
        except FileNotFoundError:
            print('`Docker` isn\'t installed... C\'mon!!!')
            sys.exit(1)

    def is_proj_init() -> None:
        mssg = f'`{proj_name}` exists!  Can proceed...'
        no_dice = f'`{proj_name}` DOESN\'T exist!!!'
        try:
            _path = f'{proj_loc}/configs/specs.yaml'
            with open(_path) as specs:
                if specs:
                    print(mssg)
        except FileNotFoundError:
            print(no_dice)
            sys.exit(1)

    def build():
        build_args = {
            '__ENV__': 'dv',
            '__FAMILY__': proj_name
        }
        cli = APIClient(base_url='unix://var/run/docker.sock')
        for line in cli.build(
                path=proj_loc,
                rm=True,
                buildargs=build_args,
                tag=tag):
            line = line.decode('utf-8')
            line = line.split('\n')
            for row in line:
                row = ''.join(row)
                try:
                    row = json.loads(row)
                    stream = row.get('stream')
                    if stream:
                        print(
                            stream
                                .replace(
                                    '\n',
                                    ''
                                )
                        )
                except JSONDecodeError:
                    pass
        print('Done!')

    def push():
        for line in client.api.push(
                tag,
                stream=True,
                decode=True
            ):
            print(
                ' '.join(
                    list(
                        str(v) for k, v in line.items()
                        if k not in {
                            'progressDetail',
                        }
                    )
                )
            )
        print('Done!')

    is_docker_installed()
    is_proj_init()
    build()
    push()


def localflow(pipeline_name: str, vals: Valuables) -> None:
    kind, sched = vals.kind, vals.schedule
    mssg, devil = f'`{ENV}-{vals.name}-{pipeline_name}` launching in `{ENV}`!', {'develop', }
    conf_body = (
        K8sPodConf()
            .env(ENV)
            .family_name(vals.name)
            .is_local(True)
            .container_registry(grab_inits()['registry'])
            .metadata(name=pipeline_name, proj_or_acct_id=vals.proj_or_acct_id, namespace='default')
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
    if ENV == 'dv' and sched in devil:
        LOGGER.info(mssg)
        launch_pod_on_local(
            conf_body
        )


def local_scheds(proj_name: str, member_name: str) -> dict:
    sched = scheds(
        grab_inits()['local_repo_dir'],
        proj_name
    )
    return {
        member_name: sched[
            member_name
        ]
    }


def local_run(proj_name: str, member_name: str) -> None:
    is_kubectl_installed()
    scheduler(
        local_scheds(proj_name, member_name),
        workflow=localflow,
        is_local=True
    )
    name = f'dv-{proj_name}-{member_name}'
    pod_status_local(name)
