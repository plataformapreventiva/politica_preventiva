#!/usr/bin/env python
# -*- coding: utf-8 -*-
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

""" Ingest Utils

This module contains utilities for python Ingestion scripts:

supports:
    raw flatfiles csv
    pandas 

"""


conf = {}
with open("../conf/conf_profile.json", "r") as f:
    conf_profile = json.load(f)
    conf["SMN_USER"] = conf_profile["SMN_USER"]
    conf["SMN_PASSWORD"] = conf_profile["SMN_PASSWORD"]
    conf["PGPORT"] = conf_profile["PGPORT"]
    conf["PGHOST"] = conf_profile["PGHOST"]
    conf["PGDATABASE"] = conf_profile["PGDATABASE"]
    conf["PGUSER"] = conf_profile["PGUSER"]
    conf["PGPASSWORD"] = conf_profile["PGPASSWORD"]
    conf["SMN_USER"] = conf_profile["SMN_USER"]
    conf["SMN_PASSWORD"] = conf_profile["SMN_PASSWORD"]



LOGGING_CONF = "../etl/sedesol_logging.conf"
logging.config.fileConfig(LOGGING_CONF)


"""def unpack_in_place(archive_dir, archive_file, archive_type="rar",password=None):
     Unrar a .rar / Unzip a zip file

    This is just a wrapper for zipfile.ZipFile and rarfile.RarFile's
    extractall  methods.

    :param [string] archive_dir The directory containing the file to extract.
    :param [string] archive_file The actual file to unrar
    :param [string] password In case the file needs a password to get unrar-ed,
     enter it here.

    :return None
    :side-effects Extracts the archive_file, creating the inflated version in rar_dir.
    :rtype None
    
    archive_path = os.path.join(archive_dir, archive_file)
    logger.info("Unpacking %s" % archive_path)

    if archive_type is "rar":
        archive_obj = rarfile.RarFile(archive_path)
    elif archive_type is "zip":
        archive_obj = zipfile.ZipFile(archive_path)

    archive_obj.extractall(archive_dir, password)
"""

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


def csv_to_db_cmds(csv_path, conf, table_name=None, schema_name=None,header_present=True):
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
    csvsql_cmd = r"head %s | tr [:upper:] [:lower:] | tr ' ' '_' | csvsql -i postgresql --tables '%s' --no-constraints --no-inference %s > %s" % (
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
    psql_load = r"cat %s | PGPASSWORD=%s psql -h %s -U %s -c '\copy %s.%s from stdin with csv header;'" %(
        csv_path,
        conf["PASSWORD"],
        conf["HOST"],
        conf["USER"],
        schema_name,
        table_name
    )

    return csvsql_cmd, schema_cmd, psql_drop, psql_create, psql_load


def csv_to_db_table(csv_path, conf, table_name=None, schema_name=None,header_present=True):
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


def geo_to_sql(shp_dir, schema_name, table_name, output_path,encoding="utf-8"):
    """Convert a directory containing shapefiles to a SQL database

    This function uses the shp2pgsql command in Postgis to convert a
    directory containing shapefiles into a single SQL database, which
    can be uploaded to Postgres using, for example,

    psql -h cluster -U username -W -f manzanas.sql

    :param shp_dir [string] path to directory with  all the shapefiles
    :param schema name [string] String specifying the schema to which
     the new table belongs.
    :param table_name [string] The name the table to create using the
      shape files in shp_dir.
    :param output_path [string] The path and name of sql file to which to
     output results.
    :param encoding [string] The character encoding of the underlying
     dbfs.
    :return None
    :side-effects The merged SQL database is create in the path
     specified by output_name. The intermediate SQL files are located
     in the original shapefile directory.
    """
    all_files = os.listdir(shp_dir)
    all_files = set([shp_dir + "/" + f.split(".")[0] for f in all_files])
    all_files = list(all_files)
    all_files.sort()

    # loop over shapefiles, using -a to describe files that will be appended
    for (cur_ix, cur_file) in enumerate(all_files):
        append_str = ""
        if cur_ix != 0:
            append_str = "-a"
        shp2pgsql_cmd = "shp2pgsql -W '%s' %s %s %s.%s > %s" % \
                        (encoding, append_str, cur_file, schema_name,
                         table_name, cur_file + ".sql")
        logger.info(shp2pgsql_cmd)
        os.system(shp2pgsql_cmd)

    # combine the intermediate SQL databases
    os.system("cat %s/*.sql > %s" % (shp_dir, output_path))


def load_geo(shp_dir, conf, schema_name, table_name, encoding="utf-8"):
    """Load spatial data to a postgres database

    This is a wrapper of geo_to_sql() that also uploads the final sql file
    into a postgres database. The actual sql file generated by geo_to_sql()
    is only temporary.

    :param shp_dir [string] path to directory with  all the shapefiles
    :param schema name [string] String specifying the schema to which
     the new table belongs.
    :param conf [dict] A dict specifying information needed to connect to
     and load to the database. Expects the fields "USER", "PASSWORD",
     "DATA_PASSWORD", and "HOST".
    :param table_name [string] The name the table to create using the
      shape files in shp_dir.
    :param output_path [string] The path and name of sql file to which to
     output results.
    :param encoding [string] The character encoding of the underlying
     dbfs.
    :return None
    :side-effects The merged SQL database is uploaded to the database.
    """
    _, temp_output = tempfile.mkstemp()
    geo_to_sql(shp_dir, schema_name, table_name, temp_output, encoding)

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
        temp_output
    )

    for cmd in [psql_drop, psql_create]:
        logger.info(cmd)
        os.system(cmd)
    os.remove(temp_output)


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


def load_spatial_from_archive(archive_dir, conf, schema_name, table_name,encoding="utf-8", archive_type="rar"):
    """ Given a directory of compressed files containing spatial data,
    upload the unarchived data to a database

    This is a wrapper for load_geo that first unarchives the required
    data, moves the shapefiles to a subdirectory, and loads the
    resulting sql to a database.

    :param archive_dir [string] An absolute path to the
    :param conf [dict] A dict specifying information needed to connect to
     and load to the database. Expects the fields "USER", "PASSWORD",
    "DATA_PASSWORD", and "HOST". See load_geo() describing how the
     connection is established.
    :param schema_name [string] The name to the (assumed existing) schema to
     add the table to. Defaults to the schema name.
    :param table_name [string] The name the table to create using the
      shape files in shp_dir.
    :param [string] archive_file The actual file to unarchive
    :param encoding [string] The character encoding of the underlying
     dbfs.
    :return None
    :rtype None
    :side-effects: Unarchives the archive_dir and uploads data to the postgres database.
    """
    unpack_all_in_dir(archive_dir, archive_type)

    # move shapefiles to their own directories
    shp_ext = ["dbf", "prj", "dbn", "sbx", "shp", "shx", "sbn", "csv"]
    unpacked_shps = [x for x in os.listdir(archive_dir)
                     if x.endswith(tuple(shp_ext))]
    move_to_subdir(unpacked_shps, archive_dir, "unpacked_shps")

    # load to a database
    load_geo(os.path.join(archive_dir, "unpacked_shps"), conf, schema_name,
             table_name, encoding)


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
    sent = k.set_contents_from_file(file, cb=callback, md5=md5, reduced_redundancy=reduced_redundancy, rewind=True)

    # Rewind for later use
    file.seek(0)

    if sent == size:
        return True
    return False


def maybe_download(url,filename, force=False):
  """Download a file if not present in path."""
  if force or not os.path.exists(filename):
    print('Attempting to download:', filename) 
    filename, _ = urlretrieve(url + filename, filename, reporthook=download_progress_hook)
    print('\nDownload Complete!')
  statinfo = os.stat(filename)
  print('Expected file size', filename)
  return filename
