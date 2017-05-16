"""
Utilities used throughout the sedesol luigi pipeline
"""

import json
import os
import re
import hashlib
import string
import datetime
import logging
import logging.config
import psycopg2
import numpy as np
import pandas as pd
from pandas.compat import range, lzip, map

import luigi
import luigi.postgres
from luigi import configuration
from luigi import six


LOGGING_CONF = configuration.get_config().get("core", "logging_conf_file")
logging.config.fileConfig(LOGGING_CONF)
logger = logging.getLogger("sedesol.pipeline")




def db_connection():
    """
    Connect to the sedesol database

    This connects to the sedesol database using the parameters in the luigi.cfg
    file.

    :return connection The connection object resulting after connecting to the
     sedesol database with the appropriate parameters.
    """
    conf = configuration.get_config()

    # connect to the postgres database
    with open(conf.get('postgres', 'db_profile')) as json_file:
        db_profile = json.load(json_file)

    port = db_profile["PGPORT"]
    host = db_profile["PGHOST"]
    database = db_profile["PGDATABASE"]
    user = db_profile["PGUSER"]
    password = db_profile["PGPASSWORD"]

    conn_string = "host='%s' port='%s' dbname='%s' user='%s' password='%s'" % (
        host, port, database, user, password
    )
    conn = psycopg2.connect(conn_string)
    conn.autocommit = True
    return conn


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


###### New Functions