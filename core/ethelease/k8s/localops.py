#!/usr/bin/env python

import sys
import yaml
from time import sleep
from kubernetes import client, config, utils
from kubernetes.client.exceptions import ApiException
from ethelease.commons.utils import home_dir, LOGGER


def is_kubectl_installed() -> None:
    try:
        path = '.kube/config'
        with open(f'{home_dir()}/{path}') as conf:
            conf = yaml.safe_load(conf)
            if conf:
                LOGGER.info('`kubectl` found!  Can proceed...')
    except FileNotFoundError:
        LOGGER.info('`kubectl` isn\'t installed!!!')
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
        LOGGER.info(f'`{name}` launched on `{get_curr_cntxt()}`')
    except ApiException as e:
        LOGGER.info(f'`{name}` didn\'t launch!')
        sys.exit(1)


def pod_status_local(name: str) -> None:
    config.load_kube_config()
    v1, i, inc = client.CoreV1Api(), 0, 2
    while True:
        try:
            kwargs = dict(name=name, namespace='default')
            phase = v1.read_namespaced_pod_status(**kwargs).status.phase
            if phase in {'Succeeded', }:
                LOGGER.info('Succeeded!')
                break
            elif phase in {'Pending', }:
                LOGGER.info('Pending...')
            elif phase in {'Running', }:
                LOGGER.info('Running...')
            elif phase in {'Failed', }:
                LOGGER.info('Failed!')
                break
            else:
                LOGGER.info('Unknown?')
        except ApiException:
            if i > 5:
                LOGGER.info('k8s API exception!')
                break
        sleep(inc)
        i += inc