#!/usr/bin/env python

import sys
import yaml
from time import sleep
from kubernetes import client, config, utils
from kubernetes.client.exceptions import ApiException
from ethelease.commons.utils import home_dir


def is_kubectl_installed() -> None:
    try:
        path = '.kube/config'
        with open(f'{home_dir()}/{path}') as conf:
            conf = yaml.safe_load(conf)
            if conf:
                print('`kubectl` found!  Can proceed...')
    except FileNotFoundError:
        print('`kubectl` isn\'t installed!!!')
        sys.exit(1)


def get_curr_cntxt() -> str:
    path = '.kube/config'
    with open(f'{home_dir()}/{path}') as conf:
        conf = yaml.safe_load(conf)
        curr_cntxt = conf.get('current-context')
        if curr_cntxt:
            return curr_cntxt


def launch_pod_on_local(body: dict) -> None:
    config.load_kube_config()
    api, name = client.ApiClient(), body['metadata']['name']
    try:
        utils.create_from_dict(
            api,
            data=body,
            namespace='default'
        )
        print(f'`{name}` launched on `{get_curr_cntxt()}`')
    except ApiException as e:
        print(f'`{name}` didn\'t launch!')
        sys.exit(1)


def pod_status_local(name: str) -> None:
    config.load_kube_config()
    v1, i, inc = client.CoreV1Api(), 0, 5
    while True:
        try:
            kwargs = dict(name=name, namespace='default')
            phase = v1.read_namespaced_pod_status(**kwargs).status.phase
            if phase in {'Succeeded', }:
                print('Succeeded!')
                break
            elif phase in {'Pending', }:
                print('Pending...')
            elif phase in {'Running', }:
                print('Running...')
            elif phase in {'Failed', }:
                print('Failed!')
            else:
                print('Unknown?')
        except ApiException:
            print('k8s API exception!')
            break
        sleep(inc)
