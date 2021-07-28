#!/usr/bin/env python

import argparse
from ethelease.workflow.local.run import local_docker_build, local_run
from ethelease.makeitso.builder import create_build_trigger
from ethelease.makeitso.commons.utils import grab_inits
from ethelease.makeitso.inits import make_init_conf
from ethelease.makeitso.puncher import (
    copy_files,
    maker,
    put_inits,
    project_location,
    render_builder_yaml,
    render_setup_py,
    render_scheduler_yaml
)


OPTIONAL = {'--gcp-project-id', '--gcp-zone', }

def _build_local(args: argparse) -> None:
    if args:
        local_docker_build(
            args.proj_name_for_build,
            args.proj_member_name,
        )


def _init(args: argparse) -> None:
    if args:
        make_init_conf(
            which_cloud=args.which_cloud,
            k8s_name=args.k8s_name,
            registry=args.registry,
            repo_owner=args.repo_owner,
            repo_name=args.repo_name,
            local_repo_dir=args.local_repo_dir,
            gcp_project_id=args.gcp_project_id,
            gcp_zone=args.gcp_zone
        )


def _k8sapply(args: argparse) -> None:
    if args:
        local_run(
            args.proj_name_for_k8s
        )


def _punch_it(args: argparse) -> None:
    if args:
        where, name = grab_inits()['local_repo_dir'], args.project_name
        proj_loc = project_location(name, where)
        maker(proj_loc)
        copy_files(proj_loc)
        put_inits(proj_loc)
        render_builder_yaml(proj_loc, name)
        render_setup_py(proj_loc, name)
        render_scheduler_yaml(proj_loc, name)


def _trigger_it(args: argparse) -> None:
    if args:
        create_build_trigger(
            args.name,
            args.proj_name_for_trigger
        )


def arghs(what: str) -> list:
    inits = [
        '--which-cloud',
        '--k8s-name',
        '--registry',
        '--repo-owner',
        '--repo-name',
        '--local-repo-dir',
    ]
    inits.extend(OPTIONAL)
    return dict(
        buildlocal=[
            '--proj-member-name',
            '--proj-name-build',
        ],
        init=inits,
        k8sapply=['--proj-name-k8s', ],
        punch=['--project-name', ],
        trigger=[
            '--name',
            '--proj-name-trigger',
        ]
    )[what]


WHATS = dict(
    buildlocal=_build_local,
    init=_init,
    k8sapply=_k8sapply,
    punch=_punch_it,
    trigger=_trigger_it,
)


def _iter_subpars_n_args(subpars: argparse) -> None:
    subparsers = {k: None for k in WHATS}
    for what in WHATS:
        subparsers[what] = subpars.add_parser(what)
        for arg in arghs(what):
            req = True
            if arg in OPTIONAL:
                req = False
            subparsers[what] \
                .add_argument(
                    arg,
                    required=req
                )


def _in_the_well(what: str, redashed: set) -> set:
    return redashed.intersection(
        set(x for x in arghs(what))
    )


def main() -> None:
    parser = argparse.ArgumentParser()
    subpars = parser.add_subparsers()
    _iter_subpars_n_args(subpars)
    args = parser.parse_args()
    if not args.__dict__:
        print('This won\'t do anthing...')
    redashes = set(
        f'--{k.replace("_","-")}'
        for k, _ in vars(args).items()
    )
    for what, run in WHATS.items():
        if _in_the_well(what, redashes):
            run(
                args
            )


if __name__ == '__main__':

    main()
