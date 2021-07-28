#!/usr/bin/env python

'''

MOST LIKELY DEPRECATED!!! -Will aka WillemRvX

'''


import os
from boto3 import client, resource
from io import BytesIO
from ethelease.commons.utils import load_dotenv_vars, local_kwargs


load_dotenv_vars()


def download_file_from_s3(s3client, bucket, specfile):
    infile_obj = BytesIO()
    kwargs = dict(Bucket=bucket, Key=specfile, Fileobj=infile_obj)
    s3client.download_fileobj(**kwargs)
    return infile_obj


def make_proper_aws_env(source):
    source = source.upper()
    setters = dict(AWS_ACCESS_KEY_ID=os.environ[f'{source}_AWS_ACCESS_KEY_ID'],
                   AWS_SECRET_ACCESS_KEY=os.environ[f'{source}_AWS_SECRET_ACCESS_KEY'])
    for k, v in setters.items():
        os.environ[k] = v


def s3_client():
    return client('s3')


def s3_resource():
    return resource('s3')


def s3_object_iterator(s3resource, bucket, prefix):
    """ Will get the lines of a specific object i.e. a file """
    return (
        s3resource
            .Object(bucket, prefix)
            .get()['Body']
            .iter_lines()
    )


def s3_objects_generator(s3client, bucket, prefix):
    """ Will generate objects in a bucket prefix combo """
    pages = s3client.get_paginator('list_objects_v2').paginate(Bucket=bucket, Prefix=prefix)
    for page in pages:
        if page.get('Contents'):
            for obj in page.get('Contents'):
                yield obj.get('Key')


def s3_objects_list(s3client, bucket, prefix):
    return list(
        obj for obj
        in s3_objects_generator(
            **local_kwargs(locals())
        )
    )
