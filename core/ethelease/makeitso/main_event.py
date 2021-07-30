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


def build_local(args: argparse) -> None:
    if args:
        local_docker_build(
            args.project_name,
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


def k8sapply(args: argparse) -> None:
    if args:
        local_run(
            args.project_name,
            args.proj_member_name,
        )


def punch_it(args: argparse) -> None:
    if args:
        where, name = grab_inits()['local_repo_dir'], args.project_name
        proj_loc = project_location(name, where)
        maker(proj_loc)
        copy_files(proj_loc)
        put_inits(proj_loc)
        render_builder_yaml(proj_loc, name)
        render_setup_py(proj_loc, name)
        render_scheduler_yaml(proj_loc, name)


def trigger_it(args: argparse) -> None:
    if args:
        create_build_trigger(
            args.name,
            args.project_name
        )


def arghs(what: str) -> list:
    comm_args = '--project-name'
    inits = [
        '--which-cloud',
        '--k8s-name',
        '--registry',
        '--repo-owner',
        '--repo-name',
        '--local-repo-dir',
    ]
    inits.extend(OPTIONAL)
    contained = [
        comm_args,
        '--proj-member-name',
    ]
    return dict(
        buildlocal=contained,
        init=inits,
        k8sapply=contained,
        punch=[comm_args, ],
        trigger=[comm_args, '--name', ]
    )[what]


WHATS = dict(
    buildlocal=build_local,
    init=init,
    k8sapply=k8sapply,
    punch=punch_it,
    trigger=trigger_it,
)


def iter_subpars(subpars: argparse) -> None:
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
        subparsers[what] \
            .set_defaults(
                func=WHATS[what]
            )


def main() -> None:
    parser = argparse.ArgumentParser()
    subpars = parser.add_subparsers()
    iter_subpars(subpars)
    args = parser.parse_args()
    if not args.__dict__:
        print('This won\'t do anthing...')
    args.func(args)


if __name__ == '__main__':

    main()
