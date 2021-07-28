#!/usr/bin/env python

from ethelease.makeitso.commons.utils import home_dir


def make_init_conf(which_cloud: str, k8s_name: str,
                   registry: str, repo_owner: str, repo_name: str, local_repo_dir: str,
                   gcp_project_id: str, gcp_zone: str) -> None:
    if which_cloud not in {'aws', 'gcp'}:
        raise Exception(
            'Wrong cloud bub! Only `aws`, `gcp` are supported...'
        )
    confs = dict(
        cloud=which_cloud,
        k8s_name=k8s_name,
        local_repo_dir=local_repo_dir,
        registry=registry,
        repo_owner=repo_owner,
        repo_name=repo_name,
    )
    if gcp_project_id and which_cloud == 'gcp':
        confs.update(
            dict(
                gcp_project_id=gcp_project_id,
                gcp_zone=gcp_zone,
            )
        )
    confs = '\n'.join(
        list(
            f'{k}: {v}'
            for k, v in confs.items()
        )
    )
    where_at = f'{home_dir()}/.ethel'
    with open(where_at, 'w') as fout:
        fout.write(
            f'{confs}\n'
        )
