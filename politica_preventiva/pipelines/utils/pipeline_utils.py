#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
Utilities used throughout SEDESOL pipeline
"""

import os
import string
import datetime
import psycopg2
import logging
import numpy as np
import pandas as pd
import pdb
import unicodedata
import luigi
import boto3

import datetime as dt
from luigi import configuration
from luigi import six
from itertools import product
from io import StringIO
from configparser import ConfigParser, NoOptionError, NoSectionError
from dotenv import load_dotenv, find_dotenv
from io import BytesIO
load_dotenv(find_dotenv())

# AWS
aws_access_key_id = os.environ.get('AWS_ACCESS_KEY_ID')
aws_secret_access_key = os.environ.get('AWS_SECRET_ACCESS_KEY')

s3 = boto3.client('s3', aws_access_key_id=aws_access_key_id,
                  aws_secret_access_key=aws_secret_access_key,
                  region_name="us-west-2")
# Logger
logging_conf = configuration.get_config().get("core", "logging_conf_file")
logging.config.fileConfig(logging_conf)
logger = logging.getLogger("dpa-sedesol")


def s3_to_pandas(Bucket, Key, sep="|", header=False, python_3=True):
    """
    Downloads csv from s3 bucket into a pandas Dataframe
    Assumes aws keys as environment variables
    """

    aws_secret_access_key = os.environ.get('AWS_SECRET_ACCESS_KEY')
    s3 = boto3.client('s3', aws_access_key_id=aws_access_key_id,
        aws_secret_access_key=aws_secret_access_key)
    #s3 = boto3.resource('s3')
    if python_3:
        return pd.read_csv("s3://"+Bucket+"/"+Key,sep=sep)

    else:
        obj = s3.get_object(Bucket=Bucket, Key=Key)
        return pd.read_csv(BytesIO(obj['Body'].read()), sep=sep)


def pandas_to_s3(df, Bucket, Key, sep="|"):
    """
    Adds a pandas dataframe (df) to a csv file in an s3 bucket with
    the specified key.
    """
    s3 = boto3.resource('s3', aws_access_key_id=aws_access_key_id,
                        aws_secret_access_key=aws_secret_access_key)

    s = StringIO()
    df.to_csv(s, sep=sep, index=False)

    s3.Object(Bucket, Key).put(Body=s.getvalue())


def copy_s3_files(input_bucket, input_key, output_bucket, output_key):
    """
    Copy from one bucket to another
    """
    s3 = boto3.resource('s3')
    copy_source = {
        'Bucket': input_bucket,
        'Key': input_key
    }
    s3.meta.client.copy(copy_source, output_bucket, output_key)


def delete_s3_file(Bucket, Key):
    """
    Delete s3 file
    """
    client = boto3.client('s3')
    response = client.delete_object(Bucket=Bucket, Key=Key)
    return response


def get_s3_file_size(Bucket, Key):
    """
    Get file size from s3
    """
    s3 = boto3.resource('s3')
    obj = s3.Object(Bucket, Key)
    return obj.content_length


def parse_cfg_list(string):
    """
    Parse string from cfg into a list
    """
    string = string.split(",")
    return [m.strip() for m in string]


def get_extra_str(string):
    """
    """
    if len(string) > 0:
        extra_h = "--" + string
    else:
        extra_h = ""
    return extra_h


def look_for_end_date(pipeline, current_date):
    """
    Check if start date is defined in luigi.csf if not return current_date
    """
    try:
        return configuration.get_config().get(pipeline, 'start_date')

    except:

        return current_date


def current_quarter(dt):
    """
    This function returns a string with the year quarter
        January, February and March (Q1); 
        April, May and June (Q2); 
        July, August and September (Q3); 
        October, November and December (Q4). 
    """
    return int((dt.month  - 1) / 3 + 1)


def year_fraction(periods, start_date, end_year, suffix):
    """
    Creates a list with year fractions (quarter, semester, weeks)
    Args:
      (periods): List of posible year fractions
      (start_date): Str YYYY or YYYY-MM
      (start_year): Int YYYY
      (end_year): Int YYYY
    Returns:
      (dates): list containing year fractions.
    """
    end_period = int(suffix.split('-')[1])

    try:
        start_year = int(start_date.split('-')[0])
        start_period = int(start_date.split('-')[1])
    except:
        # Start at the beginning of the year
        start_period = 1

    years = list(range(start_year, end_year + 1))
    return [str(year) + '-' + str(period) for year, period in
            product(years, periods) if (period <= end_period or
            year < end_year) and
            (period >= start_period or year > start_year)]


def dates_list(pipeline, end_date, periodicity):
    """
    Creates intructions for the date_construction function to create a list
    of pipeline_task dates to run.
    """
    end_year = end_date.year
    try:
        if periodicity in ["annual", 'a']:
            suffix = 'a'
            end_date = end_date.strftime('%Y')
            start_date = look_for_end_date(pipeline, end_date)
            dates = [str(year) for year in range(int(start_date), end_year + 1)]

        elif periodicity in ["biannual", 'b']:

            if end_date.month <= 6:
                suffix = 'b-1'
                end_date = end_date.strftime('%Y')
                start_date = look_for_end_date(pipeline, end_date)
                p = (1, 2)
                dates = year_fraction(p, start_date, end_year, suffix)
            else:
                suffix = 'b-2'
                end_date = end_date.strftime('%Y')
                start_date = look_for_end_date(pipeline, end_date)
                p = (1, 2)
                dates = year_fraction(p, start_date, end_year, suffix)

        elif periodicity in ["quarterly", 'q']:
            suffix = 'q-' + str(current_quarter(end_date))
            end_date = end_date.strftime('%Y')
            start_date = look_for_end_date(pipeline, end_date)
            p = (1, 2, 3, 4)
            dates = year_fraction(p, start_date, end_year, suffix)

        elif periodicity in ["monthly", 'm']:
            suffix = 'm-' + str(end_date.month)
            end_date = end_date.strftime('%Y')
            start_date = look_for_end_date(pipeline, end_date)
            p = list(range(1, 13))
            dates = year_fraction(p, start_date, end_year, suffix)

        elif periodicity in ["weekly", "w"]:
            suffix = 'w-' + str(end_date.isocalendar()[1])
            end_date = end_date.strftime('%Y-%m')
            start_date = look_for_end_date(pipeline, end_date)
            p = list(range(1, 52))
            dates = year_fraction(p, start_date, end_year, suffix)

        else:
            logger.exception("""Periodicity is not correctly defined, check
                             Luigi.cfg and choose one [a, b, q, m, w]""")

        suffix = suffix.split('-')[0]
        return (dates, suffix)

    except(NoOptionError):
        return [end_date]

def final_dates(historical, pipeline_task, current_date):
    periodicity = configuration.get_config().get(pipeline_task,
                                                 'periodicity')
    if periodicity == 'None':
        dates = 'na'
        suffix = 'fixed'
        return (dates, suffix) 

    elif historical in ['True', 'T', '1', 'TRUE']:
        logger.info('Preparing to get historic data for the pipeline_task: {0}'.\
                    format(pipeline_task))
        dates, suffix = dates_list(pipeline_task,
                                        current_date,
                                        periodicity)
        logger.debug('Pipeline task {pipeline} has historic dates {hdates}'.\
                     format(pipeline=pipeline_task, hdates=dates))
    else:
        logger.info('Preparing to get current data for'+ \
                    ' the pipeline_task: {0}'.format(pipeline_task))
        dates, suffix = dates_list(pipeline_task,
                                        current_date,
                                        periodicity)
        dates = dates[-2:]
        logger.debug('Pipeline task {pipeline} has dates {hdates}'.\
                     format(pipeline=pipeline_task, hdates=dates))
    try:
        # if the pipeline_Task has start_date
        configuration.get_config().get(pipeline_task,
                                       'start_date')
        try:
            skip = [x.strip() for x in configuration.get_config().get(pipeline_task,
                                                                      'skip').split(',')]
        except:
            skip = []
        lista = []
        for x in dates:
            if x not in skip:
                 lista.append(x)
        return (lista, suffix)

    except:
        logger.info('Start date is not defined for the pipeline {0}'.format(
            pipeline_task)+\
            'Luigi will get only the information of the last period')
def extras(pipeline):
    # pdb.set_trace()
    try:
        param = configuration.get_config().get(pipeline, "extra_parameters")
        extra = parse_cfg_list(configuration.get_config().get(pipeline, param))
        return extra

    except (NoOptionError):
        return ['']


class TableCopyToS3(luigi.Task):
    """Dump a table from postgresql to S3."""
    table_name = luigi.Parameter()
    s3_path = luigi.Parameter()

    def output(self):
        return luigi.s3.S3Target(self.s3_path)

    def run(self):
        postgres_url = os.environ['POSTGRES_URL']
        url_parts = urlparse.urlparse(postgres_url)

        conn = psycopg2.connect(
            host=url_parts.hostname,
            port=url_parts.port,
            user=url_parts.username,
            password=url_parts.password,
            dbname=url_parts.path[1:])

        with self.output().open('w') as s3_file:
            conn.cursor().copy_to(s3_file, self.table_name)

        conn.close()


def remove_accents(input_str):
    nfkd_form = unicodedata.normalize('NFKD', input_str)
    only_ascii = nfkd_form.encode('ASCII', 'ignore')
    return only_ascii


def lower(df, column):
    df[column] = df[column].map(lambda x: x if type(x) != str else x.lower())
    return df


def strip_accents(text):
    """
    Strip accents from input String.

    :param text: The input string.
    :type text: String.

    :returns: The processed String.
    :rtype: String.
    """
    try:
        text = unicode(text, 'utf-8')
    except NameError:  # unicode is a default on python 3
        pass
    text = unicodedata.normalize('NFD', text)
    text = text.encode('ascii', 'ignore')
    text = text.decode("utf-8")
    return str(text)


def cve_loc_construct(cve_ent, cve_mun, cve_loc):
    try:
        cve_ent = str(int(cve_ent)).zfill(2)
        cve_mun = str(int(cve_mun)).zfill(3)
        cve_loc = str(int(cve_loc)).zfill(4)
        cve_locc = cve_ent + cve_mun + cve_loc
        cve_mun = cve_ent + cve_mun

    except:
        cve_locc = ""
    return pd.Series({'cve_ent': cve_ent, 'cve_mun': cve_mun, 'cve_locc': cve_locc})
