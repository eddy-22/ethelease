#!/usr/bin/env python

import os
import pkgutil

import yaml
from jinja2 import Environment, BaseLoader
from pathlib import Path
from ethelease.makeitso.commons.utils import grab_inits, home_dir


def project_location(name: str, where: str) -> str:
    loc = where.replace('~', home_dir())
    return f'{loc}/{name}'


def render_base(name: str, tmpl_outpath: str, file_name: str, rend_dict: dict = None) -> None:
    with open(tmpl_outpath, 'w') as fout:
        kwargs = dict(loader=BaseLoader(), trim_blocks=True)
        data = pkgutil.get_data(__name__, f'templates_n_files/{file_name}.tmpl').decode()
        if not rend_dict: rend_dict = dict()
        rend_dict.update(
            dict(
                bracks_l='{{',
                bracks_r='}}',
                family=name,
            )
        )
        fout.write(
            Environment(**kwargs)
                .from_string(data)
                .render(
                    **rend_dict
                )
        )


def render_builder_yaml(proj_loc: str, family_name: str) -> None:
    builders, init = dict(gcp='cloudbuild.yaml', aws='codebuild.yaml'), grab_inits()
    cloud = init['cloud']
    file_name = builders[cloud]
    rend_dict = dict(k8s=init['k8s_name'], )
    if cloud == 'gcp':
        rend_dict.update(dict(gcp_zone=init.get('gcp_zone'), ))
    render_base(
        family_name.replace('_', '-'),
        tmpl_outpath=f'{proj_loc}/{file_name}',
        file_name=file_name,
        rend_dict=rend_dict
    )


def render_scheduler_yaml(proj_loc: str, family_name: str) -> None:
    file_name, inits = 'scheduler.yaml', grab_inits()
    cloud, render_dict = inits['cloud'], None
    if cloud == 'gcp':
        acct_or_proj_id = inits['gcp_project_id']
        render_dict = dict(image=f'{acct_or_proj_id}/{family_name}')
    render_base(
        family_name.replace('_', '-'),
        tmpl_outpath=f'{proj_loc}/{file_name}.tmpl',
        file_name=file_name,
        rend_dict=render_dict
    )


def render_setup_py(root: str, integration_name: str) -> None:
    file_name = 'setup.py'
    render_base(
        integration_name,
        tmpl_outpath=f'{root}/{file_name}',
        file_name=file_name
    )


def copy_files(proj_loc: str) -> None:
    init, whence = grab_inits(), 'templates_n_files'
    cloud = init['cloud']
    dockers = dict(gcp='Dockerfile.gcp', aws='Dockerfile.aws')
    files = ['requirements.txt', 'render.go', ]
    files.extend(
        [dockers.get(cloud), ]
    )
    for f in files:
        goods = (
            pkgutil
                .get_data(__name__, f'{whence}/{f}')
                .decode()
        )
        if f:
            if f.find('Dockerfile') != -1:
                f = 'Dockerfile'
            with open(f'{proj_loc}/{f}', 'w') as fout:
                fout.write(
                    goods
                )


def put_inits(proj_loc: str):
    inits = grab_inits()
    with open(f'{proj_loc}/configs/inits.yaml', 'w') as fout:
        inits.pop('repo_owner')
        inits.pop('repo_name')
        fout.write(
            yaml.safe_dump(
                inits
            )
        )


def maker(proj_loc: str) -> None:
    path = proj_loc
    subpaths = dict(configs='specs.yaml', pipelines='etl.py', )
    os.makedirs(path, exist_ok=False)
    for p, f in subpaths.items():
        sub = f'{path}/{p}'
        os.makedirs(sub, exist_ok=False)
        if f:
            filer = Path(f'{sub}/{f}')
            filer.touch(
                exist_ok=False
            )
