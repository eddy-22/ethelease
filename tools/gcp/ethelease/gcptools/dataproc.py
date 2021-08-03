#!/usr/bin/env python


# NEEDS WORK


import json
import os
from io import BytesIO
from time import sleep
from google.auth import default
from googleapiclient.discovery import build
from ethelease.commons.utils import LOGGER
from gcptools.ethelease.gcptools.cloudstorage import write_file_to_gcs


if os.environ.get('GOOGLE_APPLICATION_CREDENTIALS'):
    CREDS, _ = default(scopes=['https://www.googleapis.com/auth/cloud-platform'])
    DATAPROC = build('dataproc', 'v1', credentials=CREDS)
else:
    DATAPROC = None


def push_pyspark_script_to_gcs(bucket_name: str, script_path: str, script: str) -> None:
    with open(f'{script_path}/{script}', 'rb') as _script:
        infile_obj = BytesIO(_script.read())
        write_file_to_gcs(
            bucket=bucket_name,
            blob=f'pyspark-scripts/{script}',
            file_obj=infile_obj
        )


def create_cluster(name: str, project_id: str, region: str, zone: str, bucket: str, master_kind: str,
                   worker_kind: str, n_workers: int, n_preemptibles: int) -> None:
    cluster_conf = (
        CreateClusterData()
            .project_id(project_id)
            .cluster_name(name)
            .set_gce_cluster_conf(project_id, region, zone)
            .config_bucket(bucket)
            .software_version(version='2.0-debian10')
            .master_conf(machine_kind=master_kind)
            .worker_conf(machine_kind=worker_kind, n_workers=n_workers)
            .preemptible_conf(n_preemptibles=n_preemptibles)
            .assemble()
    )
    kwargs = dict(
        projectId=project_id,
        region=region,
        body=cluster_conf
    )
    if DATAPROC:
        (
            DATAPROC
                .projects()
                .regions()
                .clusters()
                .create(**kwargs)
                .execute()
        )
        LOGGER.info(f'Cluster {name} created.')


def delete_cluster(name: str, project_id: str, region: str):
    kwargs = dict(projectId=project_id, region=region, clusterName=name)
    if DATAPROC:
        (
            DATAPROC
                .projects()
                .regions()
                .clusters()
                .delete(**kwargs)
                .execute()
        )
        LOGGER.info(f'Cluster {name} deleted.')


def list_clusters(project_id: str, region: str) -> list:
    if DATAPROC:
        result = (
            DATAPROC
                .projects()
                .regions()
                .clusters()
                .list(
                    projectId=project_id,
                    region=region
                )
                .execute()
        )
        if result:
            return result['clusters']


def scale_cluster(name: str, project_id: str, region: str, n_workers: int) -> None:
    cluster_data = {'config': {'workerConfig': {'numInstances': n_workers}}}
    kwargs = dict(
        projectId=project_id,
        region=region,
        clusterName=name,
        updateMask='config.worker_config.num_instances',
        gracefulDecommissionTimeout='3s',
        body=cluster_data)
    if DATAPROC:
        (
            DATAPROC
                .projects()
                .regions()
                .clusters()
                .patch(**kwargs)
                .execute()
        )
        LOGGER.info(f'{name} to {n_workers} workers!')


def cluster_state(name: str, project_id: str, region: str) -> bool:
    state = 'UNKNOWN'
    while state != 'RUNNING':
        if DATAPROC:
            _state = list(
                c['status']['state'] for c in list_clusters(project_id, region)
                if c['clusterName'] == name
            )
            if len(_state):
                state = _state[0]
            else:
                raise Exception('WTF is happening!?')
            if state in {'RUNNING'}:
                return True
            sleep(3)
        else:
            break


def submit_pyspark_job(pyspark_script: str, bucket: str, cluster: str, project_id: str, region: str,
                       exec_mem: str, num_execs: int, args: dict) -> str:
    job_details = (
        JobDetails()
            .project_id(project_id)
            .cluster_name(cluster)
            .submit_script(bucket=bucket, pyspark_script=pyspark_script)
            .args(args)
            .executors_conf(num_execs=num_execs, exec_mem=exec_mem)
            .assemble()
    )
    kwargs = dict(
        projectId=project_id,
        region=region,
        body=job_details
    )
    if DATAPROC and cluster_state(cluster, project_id, region):
        result = (
            DATAPROC
                .projects()
                .regions()
                .jobs()
                .submit(**kwargs)
                .execute()
        )
        job_id = result['reference']['jobId']
        LOGGER.info(f'Submitted job ID {job_id}')
        return job_id


def job_status(job_id: str, project_id: str, region: str) -> bool:
    status = 'UNKNOWN'
    while status != 'DONE':
        result = (
            DATAPROC
                .projects()
                .regions()
                .jobs()
                .get(projectId=project_id,
                     region=region,
                     jobId=job_id)
                .execute()
        )
        status = result['status']['state']
        if status in {'DONE'}:
            return True
        sleep(3)


class CreateClusterData:

    scopes = [
        'bigquery',
        'bigquery.insertdata',
        'bigtable.admin.table',
        'bigtable.data',
        'cloud.useraccounts.readonly',
        'cloud-platform',
        'devstorage.read_write',
        'logging.write',
    ]

    def __init__(self):
        self._conf = dict()
        self._cluster_name = None
        self._project_id = None
        self._scope_url = 'https://www.googleapis.com/auth'

    def set_gce_cluster_conf(self, proj_id: str, region: str, zone: str):
        self._conf['gce_cluster_config'] = dict(
            zoneUri=f'https://www.googleapis.com/compute/v1/projects/{proj_id}/zones/{region}-{zone}',
            serviceAccount=os.environ['SRVC_ACCT_EMAIL'],
            serviceAccountScopes=list(f'{self._scope_url}/{s}' for s in self.scopes),
            metadata=self._metadata(),
        )
        return self

    def _metadata(self):
        return {
            'PIP_PACKAGES': 'pandas==1.22.0',
            'gcs-connector-version': '2.2.0',
            'bigquery-connector-version': '0.18.1',
            'enable-cloud-sql-hive-metastore': 'false'
        }

    def software_version(self, version: str):
        self._conf['software_config'] = dict(imageVersion=version)
        return self

    def master_conf(self, machine_kind: str):
        self._conf['master_config'] = \
            dict(
                numInstances=1,
                machineTypeUri=machine_kind,
                diskConfig=dict(
                    bootDiskType='pd-standard',
                    bootDiskSizeGb=1024
                )
            )
        return self

    def worker_conf(self, machine_kind: str, n_workers: int):
        self._conf['worker_config'] = \
            dict(
                numInstances=n_workers,
                machineTypeUri=machine_kind,
                diskConfig=dict(
                    bootDiskType='pd-standard',
                    bootDiskSizeGb=1024
                )
            )
        return self

    def preemptible_conf(self, n_preemptibles: int):
        if n_preemptibles > 0:
            self._conf['secondary_worker_config'] = \
                dict(numInstances=n_preemptibles)
        return self

    def project_id(self, proj_id: str):
        self._project_id = proj_id
        return self

    def cluster_name(self, name: str):
        self._cluster_name = name
        return self

    def config_bucket(self, conf_bucket: str):
        self._conf['config_bucket'] = conf_bucket
        return self

    def assemble(self) -> dict:
        return dict(
            projectId=self._project_id,
            clusterName=self._cluster_name,
            config=self._conf
        )


class JobDetails:

    def __init__(self):
        self._args = dict()
        self._job = dict()
        self._placement = dict()
        self._pyspark_job = dict()
        self._props = dict()
        self._jars = list()
        self._project_id = None
        self._python_file = None

    def project_id(self, proj_id: str):
        self._project_id = proj_id
        return self

    def cluster_name(self, cluster_name: str):
        self._placement.update(dict(clusterName=cluster_name))
        return self

    def submit_script(self, bucket: str, pyspark_script: str):
        bucket, folder = bucket, 'pyspark-scripts'
        self._python_file = f'gs://{bucket}/{folder}/{pyspark_script}'
        return self

    def args(self, args: dict):
        if isinstance(args, dict):
            self._args = args
        else:
            raise Exception('Passed args must be dict!')
        return self

    def executors_conf(self, num_execs: int, exec_mem: str):
        exe = 'spark.executor'
        self._props.update({f'{exe}.memory': exec_mem})
        self._props.update({f'{exe}.instances': num_execs})
        return self

    def jars(self):
        self._jars.append('gs://spark-lib/bigquery/spark-bigquery-latest.jar')
        return self

    def _assemble_pyspark(self) -> dict:
        self.jars()
        return dict(
            mainPythonFileUri=self._python_file,
            args=[json.dumps(self._args)],
            properties=self._props
        )

    def _assemble_job(self) -> dict:
        return dict(
            placement=self._placement,
            pysparkJob=self._assemble_pyspark()
        )

    def assemble(self) -> dict:
        return dict(
            projectId=self._project_id,
            job=self._assemble_job()
        )
