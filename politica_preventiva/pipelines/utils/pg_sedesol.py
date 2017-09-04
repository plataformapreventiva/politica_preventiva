#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
Utilities used throughout the sedesol luigi pipeline
"""
import sys
import shutil
import requests
from requests.auth import HTTPDigestAuth
import boto
import json
import os
import re
import hashlib
import string
import psycopg2
import numpy as np
import pandas as pd


def maybe_download(url, filename, force=False):
    """Download a file if not present in path."""

    if force or not os.path.exists(filename):
        print('Attempting to download:', filename)
        filename, _ = urlretrieve(
            url + filename, filename, reporthook=download_progress_hook)
        print('\nDownload Complete!')

    statinfo = os.stat(filename)
    print('Expected file size', filename)

    return filename


def parse_cfg_string(string):
    """
    Parse a comma separated string into a list
    """
    string = string.split(",")
    return [m.strip() for m in string]


def hash_name(string, max_chars=32):
    """
    Return the sha1 hash of a string
    """
    hash_obj = hashlib.sha1(string.encode())
    return hash_obj.hexdigest()[:max_chars]


def hash_if_needed(string, min_hash_req=55, max_chars=32):
    """
    Return the hash to a string if it is too long
    We keep the first five characters as a prefix though.
    """
    result_string = string
    if len(string) > min_hash_req:
        vowels = re.compile('[aeiou]', flags=re.I)
        result_string = vowels.sub('', string)
    if len(result_string) > min_hash_req:
        result_string = string[:5] + hash_name(string, max_chars - 5)
    return result_string


def basename_without_extension(file_path):
    """
    Extract the name of a file from it's path
    """
    file_path = os.path.splitext(file_path)[0]
    return os.path.basename(file_path)


def get_csv_names(basename, params_dict):
    """
    Paste names in a string, after extracting basenames and removing extensions
    """
    components = [basename] + list(params_dict.values())
    components = [basename_without_extension(s) for s in components]
    components = [strip_punct(s) for s in components]
    return "-".join(components)


def strip_punct(cur_string):
    """
    Remove punctuation from a string
    """
    exclude = set(string.punctuation)
    exclude.add(' ')
    exclude.remove('_')

    regex = '[' + re.escape(''.join(exclude)) + ']'
    return re.sub(regex, '', cur_string)


def download_dir(client, resource, dist, local='/tmp', bucket='your_bucket'):

    paginator = client.get_paginator('list_objects')

    for result in paginator.paginate(Bucket=bucket, Delimiter='/', Prefix=dist):

        if result.get('CommonPrefixes') is not None:

            for subdir in result.get('CommonPrefixes'):

                download_dir(client, resource, subdir.get(
                    'Prefix'), local, bucket)

        if result.get('Contents') is not None:

            for file in result.get('Contents'):

                if not os.path.exists(os.path.dirname(local + os.sep + file.get('Key'))):

                    os.makedirs(os.path.dirname(
                        local + os.sep + file.get('Key')))

                resource.meta.client.download_file(
                    bucket, file.get('Key'), local + os.sep + file.get('Key'))


def move_to_subdir(files, base_dir, subdir_name):
    """ Move a list of files from their base directory to a subdirectory

    This is just a helper function to shift files around when ingesting data.
    This will create or replace a subdirectory of base_dir, moving all the
    files from the base_dir to the subdir.

    :param files [list of strings] A list of the filenames (without preceding
     paths) contained in the base_dir, which we want to move to the subdir.
    :param base_dir [string] An absolute path to the base directory currently
     containing the files.
    :param subdir_name [string] A string giving the folder name within the base
     dir, to which to move the files.
    :return None
    :rtype None
    :side-effects Move "files" from the base_dir to a new directory called
     subdir_name. If this folder already exists, it will be removed.
    """
    logger.info("moving files from %s to %s" % (base_dir, subdir_name))
    subdir = os.path.join(base_dir, subdir_name)
    if os.path.exists(subdir):
        shutil.rmtree(subdir)
    os.mkdir(subdir)
    [shutil.move(os.path.join(base_dir, x), subdir) for x in files]


def upload_to_s3(aws_access_key_id, aws_secret_access_key, file, bucket, key, callback=None, md5=None, reduced_redundancy=False, content_type=None):
    """
    Uploads the given file to the AWS S3
    bucket and key specified.

    callback is a function of the form:

    def callback(complete, total)

    The callback should accept two integer parameters,
    the first representing the number of bytes that
    have been successfully transmitted to S3 and the
    second representing the size of the to be transmitted
    object.

    Returns boolean indicating success/failure of upload.
    """
    try:
        size = os.fstat(file.fileno()).st_size
    except:
        # Not all file objects implement fileno(),
        # so we fall back on this
        file.seek(0, os.SEEK_END)
        size = file.tell()

    conn = boto.connect_s3(aws_access_key_id, aws_secret_access_key)
    bucket = conn.get_bucket(bucket, validate=True)
    k = Key(bucket)
    k.key = key
    if content_type:
        k.set_metadata('Content-Type', content_type)
    sent = k.set_contents_from_file(
        file, cb=callback, md5=md5, reduced_redundancy=reduced_redundancy, rewind=True)

    # Rewind for later use
    file.seek(0)

    if sent == size:
        return True
    return False
