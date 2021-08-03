#!/usr/bin/env python

import os
from google.auth import default
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError
from ethelease.commons.utils import LOGGER, warn_handles


if os.environ.get('GOOGLE_APPLICATION_CREDENTIALS'):
    CREDS, _ = default(scopes=['https://www.googleapis.com/auth/cloud-platform'])
else:
    CREDS = None


def authed(creds) -> build:
    return build(
        'cloudbuild',
        'v1',
        credentials=creds
    )


class CreateTrigger:

    __slots__ = ['_project_id',
                 '_conf',
                 'github_conf_shell_push',
                 'github_conf_shell_pr',
                 'name',
                 'triggers']

    def __init__(self):
        self._project_id = ''
        self._conf = dict()
        self.github_conf_shell_push = dict(
            push=dict(branch=''),
            owner='',
            name='',
        )
        self.github_conf_shell_pr = dict(
            pullRequest=dict(branch=''),
            owner='',
            name='',
        )
        self.name = None
        self.triggers = None

    def _github_conf_push(self, branch: str, repo_owner: str, repo_name: str):
        self.github_conf_shell_push['push']['branch'] = f'^{branch}$'
        self.github_conf_shell_push['owner'] = repo_owner
        self.github_conf_shell_push['name'] = repo_name
        self._conf['github'] = self.github_conf_shell_push

    def _github_conf_PR(self, branch: str, repo_owner: str, repo_name: str):
        self.github_conf_shell_pr['pullRequest']['branch'] = f'^{branch}$'
        self.github_conf_shell_pr['owner'] = repo_owner
        self.github_conf_shell_pr['name'] = repo_name
        self._conf['github'] = self.github_conf_shell_pr

    def github_events_config(self, env: str, args: dict):
        confs = {e: self._github_conf_push for e in {'dv', 'pr', 'qa'}}
        confs[env](**args)
        return self

    def make_sub_vars(self, sub_vars: dict):
        self._conf['substitutions'] = sub_vars
        return self

    def build_config_file(self, file_name: str):
        self._conf['filename'] = file_name
        return self

    def project_id(self, id: str):
        self._project_id = id
        return self

    def set_name(self, name: str):
        self._conf['name'] = name
        self.name = name
        return self

    def create_if_needed(self):
        kwargs = dict(
            projectId=self._project_id,
            body=self._conf
        )
        if not self.exists():
            self.triggers.create(**kwargs).execute()
        else:
            LOGGER.info(f'Creation for `{self.name}` not needed!')
        return self

    def exists(self):
        mssg = f'The trigger `{self.name}` doesn\'t exists!'
        self.triggers = authed(CREDS).projects().triggers()

        @warn_handles((HttpError,), mssg)
        def inner() -> dict:
            kwargs = dict(
                projectId=self._project_id,
                triggerId=self._conf['name']
            )
            return (
                self.triggers
                    .get(**kwargs)
                    .execute()
            )
        return inner()
