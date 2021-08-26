#!/usr/bin/env python

import sys
from copy import deepcopy
from ethelease.makeitso.commons.utils import grab_inits

try:
    from ethelease.gcptools.cloudbuild import CreateTrigger
    CAN_CREATE = True
except ImportError:
    CAN_CREATE = False


def create_cloudbuild_trigger(name: str, proj_name: str) -> None:
    inits = grab_inits()
    kwargs = dict(repo_owner=inits['repo_owner'], repo_name=inits['repo_name'])
    devl, prod, qual, _name = deepcopy(kwargs), deepcopy(kwargs), deepcopy(kwargs), name.replace('_', '-')
    devl.update(dict(branch=f'(dev\/{proj_name}[-a-zA-Z0-9]*|fix\/{proj_name}[-a-zA-Z0-9]*)'))
    prod.update(dict(branch=f'prod\/{proj_name}'))
    qual.update(dict(branch=f'qual\/{proj_name}'))
    if CAN_CREATE:
        create_trigger = (
            CreateTrigger()
                .project_id(inits['gcp_project_id'])
                .build_config_file(f'{proj_name}/cloudbuild.yaml')
        )
        for e, v in dict(dv=devl, pr=prod, qa=qual).items():
            name = f'{e}-{_name}'
            subvars = {
                '_ENV': e,
                '_FAMILY': proj_name,
                '_K8S': inits['k8s_name']
            }
            created = (
                deepcopy(create_trigger)
                    .set_name(name)
                    .make_sub_vars(subvars)
                    .github_events_config(e, v)
                    .create_if_needed()
                    .exists()
            )
            if created:
                sys.stdout.write(
                    f'Trigger {name} exists!'
                )
    else:
        sys.stdout.write(
            'Can\'t create trigger.  '
            'Need to install `gcptools`'
        )


def create_build_trigger(name: str, proj_name: str) -> None:
    triggs = dict(
        gcp=create_cloudbuild_trigger,
    )
    triggs[
        grab_inits()[
            'cloud'
        ]
    ](
        name,
        proj_name
    )
