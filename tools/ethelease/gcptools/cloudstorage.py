#!/usr/bin/env python

import json
import os
from io import BytesIO
from time import sleep
from zipfile import ZipFile, is_zipfile
from google.cloud import storage
from ethelease.commons.utils import LOGGER


if os.environ.get('GOOGLE_APPLICATION_CREDENTIALS'):
    GCS = storage.Client()
else:
    GCS = None


def create_bucket_if_needed(name: str, project_id: str, location: str) -> None:
    exists, mssg = GCS.lookup_bucket(name), f'{name} created!'
    if not exists:
        bucket = storage.Bucket(GCS, name=name)
        bucket.location, bucket.storage_class = location, 'REGIONAL'
        GCS.create_bucket(bucket, project=project_id)
        LOGGER.info(mssg)


def delete_blob(bucket_name: str, blob_name: str) -> None:
    bucket = GCS.bucket(bucket_name)
    blob = bucket.blob(blob_name)
    blob.delete()


def gcs_objects_list(bucket_name: str, prefix: str) -> tuple:
    blobs = GCS.list_blobs(bucket_name, prefix=prefix)
    return tuple(b.name for b in blobs)


def list_blob_sizes(bucket_name: str, prefix: str) -> tuple:
    blobs = GCS.get_bucket(bucket_name).list_blobs(prefix=prefix)
    return tuple(b.size for b in blobs)


def list_files(bucket_name: str, prefix: str) -> tuple:
    blobs = GCS.list_blobs(bucket_name, prefix=prefix)
    return tuple(b.name for b in blobs)


def make_object_html(bucket_name: str, blob_name: str) -> bool:
    blob = GCS.bucket(bucket_name).get_blob(blob_name)
    blob.content_type = 'text/html'
    blob.patch()
    sleep(0.5)
    if blob.content_type != 'text/html':
        return False
    return True


def read_from_gcs(bucket_name: str, blob_name: str) -> iter:
    blobs = GCS.get_bucket(bucket_name).list_blobs(prefix=blob_name)
    for blob in blobs:
        if blob.name.split('/')[-1] not in {'_SUCCESS', ''}:
            listed = (
                blob
                    .download_as_string()
                    .decode('utf-8')
                    .split('\n')
            )
            for row in listed:
                if row:
                    yield row


def read_from_gcs_hr(bucket_name: str, blob_name: str, filter_hr: str) -> iter:
    blobs = GCS.get_bucket(bucket_name).list_blobs(prefix=blob_name)
    for blob in blobs:
        tail = blob.name.split('/')[-1]
        if tail not in {'_SUCCESS', ''}:
            hr = tail.split('-')[1][0:2]
            if filter_hr == hr:
                listed = (
                    blob
                        .download_as_string()
                        .decode('utf-8')
                        .split('\n')
                )
                for row in listed:
                    if row:
                        yield row


def read_list_from_gcs(bucket_name: str, blob_name: str) -> list:
    blobs = GCS.get_bucket(bucket_name).list_blobs(prefix=blob_name)
    for blob in blobs:
        listed = (
            blob
                .download_as_string()
                .decode('utf-8')
        )
        return json.loads(listed)


def read_zipfile_from_gcs(bucket_name: str, blob_name: str) -> dict:
    zipped = BytesIO(
        GCS
            .get_bucket(bucket_name)
            .blob(blob_name)
            .download_as_string()
    )
    if is_zipfile(zipped):
        unzip = ZipFile(zipped)
        return {
            name: unzip.read(name)
            for name in unzip.namelist()
        }


def write_data_to_gcs(bucket_name: str, blob_name: str) -> callable:
    def inner(data: str, file_name: str) -> str:
        full_blob = f'{blob_name}/{file_name}'
        (
            GCS
                .get_bucket(bucket_name)
                .blob(full_blob)
                .upload_from_string(data)
        )
        LOGGER.info('Data uploaded to GCS!')
        return full_blob
    return inner


# Deprecate this...
def write_file_to_gcs(bucket: str, blob: str, file_obj) -> None:
    (
        GCS
            .get_bucket(bucket)
            .blob(blob)
            .upload_from_file(file_obj, rewind=True)
    )
    LOGGER.info('File uploaded!')
