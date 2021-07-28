#!/usr/bin/env python

from time import sleep
from kubernetes import client, config
from kubernetes.client.exceptions import ApiException
from ethelease.commons.utils import ENV, LOGGER


def darkarts(name: str, data: dict) -> None:
    config.load_incluster_config()
    v1, body = client.CoreV1Api(), K8sSecretConf().name(name).str_data(data).assemble()
    try:
        kwargs = dict(name=body['metadata']['name'], namespace='default')
        resp = v1.read_namespaced_secret(**kwargs, pretty='true')
        if resp.metadata.name == name:
            v1.delete_namespaced_secret(**kwargs)
    except ApiException:
        pass
    v1.create_namespaced_secret(
        body=body,
        namespace='default'
    )
    LOGGER.info('A dark chamber is crafted!')


def make_str_data(paths_n_filenames: dict) -> dict:
    str_data = dict()
    for p, f in paths_n_filenames.items():
        with open(p) as data:
            str_data[f] = data.read()
    return str_data


def pod_launch_n_mgmt(body: dict) -> None:

    config.load_incluster_config()
    v1, namespace, name = client.CoreV1Api(), 'default', body['metadata']['name']
    mssg = dict(
        failed=f'Pod {name} failed in {ENV}',
        launch=f'Pod {name} launched in {ENV}!',
        no_exist=f'Pod {name} doesn\'t seem to exist in {ENV}...',
        removed=f'Pod {name} removed in {ENV}!',
        run_or_pend=f'Pod {name} is running or pending in {ENV}!',
        succeed=f'Pod {name} succeeded in {ENV}!',
        unknown=f'Pod {name} status is unknown?'
    )

    def pod_status(_name: str, callback: list) -> None:
        v1, i, inc = client.CoreV1Api(), 0, 5
        while True:
            try:
                kwargs = dict(name=_name, namespace='default')
                phase = v1.read_namespaced_pod_status(**kwargs).status.phase
                if phase in {'Succeeded', }:
                    LOGGER.info(mssg['succeed'])
                    callback[0] = 'Succeeded'
                    break
                elif phase in {'Running', 'Pending', }:
                    LOGGER.info(mssg['run_or_pend'])
                    callback[0] = 'RunningOrPending'
                elif phase in {'Failed', }:
                    LOGGER.info(mssg['failed'])
                    callback[0] = 'Failed'
                else:
                    callback[0] = 'Unknown'
                    LOGGER.info(mssg['unknown'])
            except ApiException:
                callback[0] = 'NotFound'
                LOGGER.info(
                    mssg['no_exist']
                )
                break
            i += inc
            sleep(inc)

    def handle_creation(_name: str, _body: dict) -> None:
        not_found = [None, ]
        pod_status(_name, not_found)
        if not_found[0] in {'NotFound', }:
            handle_success(_name)
            v1.create_namespaced_pod(
                body=_body, namespace=namespace
            )
            LOGGER.info(mssg['launch'])

    def handle_success(_name: str) -> None:
        check_success = [None, ]
        pod_status(_name, check_success)
        if check_success[0] in {'Succeeded', }:
            v1.delete_namespaced_pod(
                name=_name,
                namespace=namespace
            )
            LOGGER.info(mssg['removed'])

    handle_creation(name, body)
    handle_success(name)


class K8sDaemonSetConf:

    def __init__(self):
        self.api_version = 'apps/v1'
        self.environ = None
        self.kind = 'DaemonSet'
        self.meta_data = dict()
        self.name_dashed = None
        self.name_underscored = None
        self._pod_spec = dict()
        self.spec = dict()

    def assemble(self) -> dict:
        self.spec = dict(
            selector=dict(matchLabels=dict(name=self.name_dashed)),
            template=dict(
                metadata=dict(labels=dict(name=self.name_dashed)),
                spec=self._pod_spec
            )
        )
        return dict(
            apiVersion=self.api_version,
            kind=self.kind,
            metadata=self.meta_data,
            spec=self.spec
        )

    def env(self, val: str):
        self.environ = val
        return self

    def metadata(self, name: str, namespace: str):
        self.name_underscored = name
        self.name_dashed = name.replace('_', '-')
        self.meta_data = dict(
            name=f'{self.environ}-{self.name_dashed}',
            namespace=namespace
        )
        return self

    def pod_spec(self, spec: dict):
        self._pod_spec = spec


class K8sJobConf:
    pass


class K8sPodConf:

    __slots__ = ['conf',
                 'api_version',
                 'kind',
                 'container_reg',
                 'familyname',
                 'image',
                 'name_dashed',
                 'name_underscored',
                 'the_script',
                 'in_args',
                 'mexpkey',
                 'nodepool_name',
                 'lim_cpu',
                 'lim_mem',
                 'req_cpu',
                 'req_mem',
                 'affinity',
                 'secret_name',
                 'restartpolicy',
                 'containers',
                 'volumes',
                 'spec',
                 'meta_data',
                 'dark_arts',
                 'environ',
                 '_is_local', ]

    def __init__(self):
        self.conf = dict()
        self.api_version = 'v1'
        self.kind = 'Pod'
        self.container_reg = None
        self.familyname = None
        self.image = None
        self.name_dashed = None
        self.name_underscored = None
        self.the_script = None
        self.in_args = None
        self.mexpkey = None
        self.nodepool_name = None
        self.lim_cpu = None
        self.lim_mem = None
        self.req_cpu = None
        self.req_mem = None
        self.affinity = None
        self.secret_name = None
        self.restartpolicy = None
        self.containers = list()
        self.volumes = list()
        self.spec = dict()
        self.meta_data = dict()
        self.dark_arts = None
        self.environ = None
        self._is_local = False

    def assemble(self) -> dict:
        self.dark_arts = f'{self.name_underscored}darkarts'.replace('_', '')
        self._affinity()
        self._containers()
        self._volumes()
        self.spec = dict(
            affinity=self.affinity,
            containers=self.containers,
            maxRetries=3,
            restartPolicy=self.restartpolicy,
            terminationGracePeriodSeconds=30,
            volumes=self.volumes
        )
        if not self.nodepool_name:
            self.spec.pop('affinity')
        if not self.secret_name:
            self.spec.pop('volumes')
        return dict(
            apiVersion=self.api_version,
            kind=self.kind,
            metadata=self.meta_data,
            spec=self.spec
        )

    def env(self, val: str):
        self.environ = val
        return self

    def container_registry(self, name: str):
        self.container_reg = name
        return self

    def family_name(self, val: str):
        self.familyname = val
        return self

    def metadata(self, name: str, namespace: str, proj_or_acct_id: str):
        self.name_underscored, self.name_dashed = name, name.replace('_', '-')
        img_name = f'{self.environ}-{self.familyname.replace("_", "-")}-{self.name_dashed}'
        if not self._is_local:
            self.image = f'{self.container_reg}/{proj_or_acct_id}/{self.environ}.{self.familyname}:latest'
        else:
            self.image = f'{self.container_reg}/{proj_or_acct_id}/{img_name}:latest'
        self.meta_data = dict(
            name=img_name,
            namespace=namespace
        )
        return self

    def pipeline_or_runner_script(self, script: str):
        self.the_script = script
        return self

    def script_args(self, args: str):
        self.in_args = args
        return self

    def which_nodepoolorgroup(self, name: str, cloud: str):
        self.mexpkey = dict(gcp='cloud.google.com/gke-nodepool')[cloud]
        self.nodepool_name = name
        return self

    def pick_secret(self, name: str):
        self.secret_name = name
        return self

    def cpu_usage(self, req: str, lim: str):
        self.req_cpu, self.lim_cpu = req, lim
        return self

    def mem_usage(self, req: str, lim: str):
        self.req_mem, self.lim_mem = req, lim
        return self

    def restart_policy(self, policy: str):
        if policy not in {'Always', 'Never', 'OnFailure'}:
            raise Exception('Can only be Always, Never, or OnFailure!')
        else:
            self.restartpolicy = policy
        return self

    def is_local(self, val: bool):
        self._is_local = val
        return self

    def _affinity(self):
        self.affinity = dict(
            nodeAffinity=dict(
                requiredDuringSchedulingIgnoredDuringExecution=dict(
                    nodeSelectorTerms=[
                        dict(matchExpressions=[
                            dict(
                                key=self.mexpkey,
                                operator='In',
                                values=[self.nodepool_name]
                            )
                        ])
                    ]
                )
            )
        )

    def _containers(self):
        comm = [
            'python',
            f'{self.familyname}/pipelines/{self.the_script}',
            '--Args',
            self.in_args
        ]
        self.containers = [
            dict(
                command=comm,
                image=self.image,
                imagePullPolicy='Always',
                name='base',
                resources=dict(
                    limits=dict(cpu=self.lim_cpu, memory=self.lim_mem),
                    requests=dict(cpu=self.req_cpu, memory=self.req_mem)
                ),
                volumeMounts=[
                    dict(
                        mountPath='/etc/secrets',
                        name=self.dark_arts,
                        readOnly=True
                    )
                ]
            )
        ]

    def _volumes(self):
        self.volumes = [
            dict(
                name=self.dark_arts,
                secret=dict(
                    defaultMode=420,
                    secretName=self.secret_name
                )
            )
        ]


class K8sSecretConf:

    __slots__ = ['chamber_lord', '_name', 'string_data']

    def __init__(self):
        self.chamber_lord = f'{ENV}-chamberofsecrets'
        self._name = None
        self.string_data = dict()

    def assemble(self):
        return dict(
            apiVersion='v1',
            kind='Secret',
            metadata=dict(name=f'{self.chamber_lord}'),
            stringData=self.string_data,
            type='Opaque'
        )

    def str_data(self, data: dict):
        self.string_data = data
        return self

    def name(self, val: str):
        self._name = val
        return self
