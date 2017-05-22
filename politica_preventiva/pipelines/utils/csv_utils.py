#!/usr/bin/env python
# -*- coding: utf-8 -*-

""" CSV Utils

This module contains utilities for Ingestion scripts for CSV files:

"""

import os
import sys
import json
import glob
import tempfile
import zipfile
import datetime
import shutil
#import rarfile
import requests
from requests.auth import HTTPDigestAuth
import logging
import logging.config
import boto
import numpy as np
import pandas as pd
from itertools import product
from bs4 import BeautifulSoup
from ftplib import FTP
from boto.s3.key import Key

def unpack_all_in_dir(archive_dir, archive_type="rar", password=None):
    """ Wrap unpack_in_place() to unpack all compressed files in a directory

    This will unzip / unrar all the compressed files in a directory, ignoring
    those that are not .zip's or .rar's.

    :param [string] archive_dir The directory containing the file to extract.
    :param [string] password In case the file needs a password to get unrar-ed,
     enter it here.

    :return None
    :side-effects Extracts all the archived files in archive_dir.
    """
    archive_rars = os.listdir(archive_dir)
    archive_rars = [x for x in archive_rars if x.endswith(archive_type)]

    for archive_file in archive_rars:
        unpack_in_place(archive_dir, archive_file, archive_type, password)


def iconv_conversion(file_path, start_enc="latin1", end_enc="utf-8"):
    """ Convert the encoding of a file using iconv

    This is just a wrapper that converts the character encoding of a file using
    iconv.

    :param file_path [string] The path to the file in the original encoding.
    :param start_enc [string] The encoding of the file at file_path.
    :param end_enc [string] The encoding to convert the file to.
    :return None
    :rtype None
    :side-effects Converts the file at file_path from start_enc encoding to
     end_enc encoding.
    """
    cmd_tuple = (file_path, start_enc, end_enc, file_path + "-utf")
    iconv_cmd = "iconv %s -f %s -t %s > %s" % cmd_tuple
    logger.info(iconv_cmd)
    os.system(iconv_cmd)

    # overwrite earlier file with new encoding
    shutil.move(file_path + "-utf", file_path)


def merge_csvs_in_dir(csv_dir, output_name):
    """ Merge all the CSVs in a directory

    This assumes they all have the same headers, and we only want to keep the first.

    :param csv_dir [str] The directory containing all the csv's to combine.
    :output_name [str] The name of the merged csv file to write.
    :return None
    :side-effects Creates a file called 'output_name' in 'csv_dir' containing
     the combined csv file.
    :rtype None
    """
    output_path = os.path.join(csv_dir, output_name)
    if os.path.isfile(output_path):
        os.remove(output_path)

    csv_files = glob.glob(os.path.join(csv_dir, "*.csv"))
    _, temp_output = tempfile.mkstemp()

    # create file using header, then merge non-header data
    header_str = "head -2 %s > %s" % (csv_files[0], temp_output)
    content_str = "tail -n +3 -q %s/*.csv >> %s" % (csv_dir, temp_output)
    cmd = header_str + ";" + content_str
    logger.info(cmd)
    os.system(cmd)
    shutil.move(temp_output, output_path)


def dbfs_to_csv(dbfs_dir):
    """ Convert all .dbf files in a directory to .csv, using ogr2ogr
    :param dbfs_dir [character] The directory containing all the .dbf files
     we want to convert.
    :return None
    :side-effects: Creates .csv files corresponding to the original .dbf, in
    the dbfs_dir/.
    :rtype None
    """
    logger.info("Converting .dbf's in %s to csv" % dbfs_dir)
    dbfs = glob.glob(os.path.join(dbfs_dir + "*.dbf"))
    for dbf_file in dbfs:
        csv_output = dbf_file.split(".")[0] + ".csv"
        cmd_paths = (os.path.join(dbfs_dir, csv_output),
                     os.path.join(dbfs_dir, dbf_file))

        if os.path.isfile(cmd_paths[0]):
            os.remove(cmd_paths[0])

        ogr_cmd = "ogr2ogr -f CSV %s %s" % cmd_paths
        logger.info(ogr_cmd)
        os.system(ogr_cmd)


def csv_to_db_cmds(csv_path, conf, table_name=None, schema_name=None, header_present=True):
    """Generate commands for uploading a CSV to a postgres database

    This automates the task of uploading csv file to an existing schema in a
    postgres database. Since this setting is general, no assumptions are made
    on the size of strings [e.g., VARCHAR instead of VARCHAR(some number)
    is used] or the non-null status. This only returns the commands as strings,
    it doesn't actually execute the commands.

    :param csv_path [string] The path to a csv file containing data to upload
     to the database.
    :param conf [dict] A dictionary containing information needed to log into
     the postgres server. it needs keys for "PASSWORD", "HOST", and "USER",
     corresponding to the fields in the man page for the psql command.
    :param table_name [string] The name to use for the table. Defaults to the
     basename of csv_path (without an extension).
    :param schema_name [string] The name to the (assumed existing) schema to
     add the table to. Defaults to the schema name.
    :param header_present [boolean] Does the csv to upload have a header line?

    :return A tuple of strings, giving a string of commands that can be
     executed to generate the table in the specified schema in the postgres
     database.
    :rtype tuple
    """
    # input defaults
    if table_name is None:
        base = os.path.basename(csv_path)
        table_name = os.path.splitext(base)[0]
    if schema_name is None:
        schema_name = table_name

    if not header_present:
        header_str = "--no-header-row"
    else:
        header_str = ""

    _, schema_path = tempfile.mkstemp()
    # construct the database schema
    csvsql_cmd = r"head %s | tr [:upper:] [:lower:] | tr ' ' '_' | \
     csvsql -i postgresql --tables '%s' --no-constraints --no-inference %s > %s" % (
        csv_path,
        table_name,
        header_str,
        schema_path
    )

    # replace table name with schema_name.table_name
    schema_cmd = r"sed -i 's/%s/%s.%s/'  %s" % (
        table_name,
        schema_name,
        table_name,
        schema_path
    )

    # drop any existing tables
    psql_drop = r"PGPASSWORD=%s psql -h %s -U %s -c 'drop table if exists %s.%s'" % (
        conf["PASSWORD"],
        conf["HOST"],
        conf["USER"],
        schema_name,
        table_name
    )

    # create the table in postgres
    psql_create = r"PGPASSWORD=%s psql -h %s -U %s -f %s" % (
        conf["PASSWORD"],
        conf["HOST"],
        conf["USER"],
        schema_path
    )

    # populate the table with data
    psql_load = r"cat %s | PGPASSWORD=%s psql -h %s -U %s -c '\copy %s.%s from stdin with csv header;'" % (
        csv_path,
        conf["PASSWORD"],
        conf["HOST"],
        conf["USER"],
        schema_name,
        table_name
    )

    return csvsql_cmd, schema_cmd, psql_drop, psql_create, psql_load


def csv_to_db_table(csv_path, conf, table_name=None, schema_name=None, header_present=True):
    """Upload a CSV file a postgres database.

    Execute commands from csv_to_db_cmds(), to upload a csv file to an existing
    postgres database schema.

    :param csv_path [string] The path to a csv file containing data to upload
     to the database.
    :param conf [dict] A dictionary containing information needed to log into
     the postgres server. it needs keys for "PASSWORD", "HOST", and "USER",
     corresponding to the fields in the man page for the psql command.
    :param table_name [string] The name to use for the table. Defaults to the
     basename of csv_path (without an extension).
    :param schema_name [string] The name to the (assumed existing) schema to
     add the table to. Defaults to the schema name.
    :param header_present [boolean] Does the csv to upload have a header line?

    :return None
    :rtype None
    :side-effects Loads data in the file at csv_path to the schema
     specified by schema_name.
    """
    logger.info("Uploading %s to %s.%s" % (csv_path, schema_name, table_name))
    cmds = csv_to_db_cmds(csv_path, conf, table_name, schema_name,
                          header_present)
    logger.info("\n".join(cmds))

    # execute those commands
    for cur_cmd in cmds:
        os.system(cur_cmd)


def csv_to_db_table_wrapper(csv_paths, conf, schema_name):
    """Load several csvs to a postgres database

    Wraps the function csv_to_db_table() to loop over several csv files,
    instead of just one.

    :param csv_paths [dict] A dictionary whose keys give table names and
     values specify the path to the csv file containing data for that table.
    :param conf [dict] A dictionary containing information needed to log into
     the postgres server. it needs keys for "PASSWORD", "HOST", and "USER",
     corresponding to the fields in the man page for the psql command.
    :param schema_name [string] The name to the (assumed existing) schema to
     add the table to. Defaults to the schema name.
    :return None
    :rtype None
    :side-effects Load data across the files specified in csv_paths to the
     schema specified by schema_name.
    """
    for (table, path) in csv_paths.items():
        csv_to_db_table(path, conf, table, schema_name)
