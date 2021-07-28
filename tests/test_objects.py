#!/usr/bin/env python

from ethelease.commons.etl_tools import PathBuilder
from ethelease.k8s.ops import K8sPodConf


def test_PathBuilder() -> None:
    target = 's3://test/env=dv/source=a-test/subsrc=part-of/yr=1970/mo=01/dy=01/kind=raw/data.txt'
    _test = (
        PathBuilder()
            .bucket(cloud='aws', name='test')
            .env('dv')
            .source('a-test')
            .subsource('part-of')
            .ds('1970-01-01')
            .kind('raw')
            .file_name('data.txt')
            .full_path()
    )
    assert _test == target


def test_K8sPodConf() -> None:
    target = {
        'apiVersion': 'v1',
        'kind': 'Pod',
        'metadata': {
            'name': 'test-test-dv',
            'namespace': 'default'
        },
        'spec': {
            'affinity': {
                'nodeAffinity': {
                    'requiredDuringSchedulingIgnoredDuringExecution': {
                        'nodeSelectorTerms': [
                            {
                                'matchExpressions': [
                                    {
                                        'key': 'test',
                                        'operator': 'In',
                                        'values': ['testjobs']
                                    }
                                ]
                            }
                        ]
                    }
                }
            },
            'containers': [
                {
                    'command': [
                        'python',
                        'test/pipelines/test',
                        '--Args', '{"test" 0}'
                    ],
                    'image': 'test/test.dv:latest',
                    'imagePullPolicy': 'Always',
                    'name': 'base',
                    'resources': {
                        'limits': {
                            'cpu': '2000m',
                            'memory': '1G'
                        },
                        'requests': {
                            'cpu': '1000m',
                            'memory': '500M'
                        }
                    },
                    'volumeMounts': [
                        {
                            'mountPath': '/etc/secrets',
                            'name': 'testdarkarts',
                            'readOnly': True
                        }
                    ]
                }
            ],
            'maxRetries': 3,
            'restartPolicy': 'Never',
            'terminationGracePeriodSeconds': 30,
            'volumes': [
                {
                    'name': 'testdarkarts',
                    'secret': {
                        'defaultMode': 420,
                        'secretName': 'chamberofsecrets'
                    }
                }
            ]
        }
    }
    _test = (
        K8sPodConf()
            .env('dv')
            .integration_name('test')
            .container_registry('test')
            .metadata(name='test', namespace='default')
            .pipeline_script('test')
            .script_args('{"test" 0}')
            .which_nodepoolorgroup(name='testjobs', matchexpkey='test')
            .pick_secret(name='chamberofsecrets')
            .cpu_usage(req='1000m', lim='2000m')
            .mem_usage(req='500M', lim='1G')
            .restart_policy('Never')
            .assemble()
    )
    assert _test == target
