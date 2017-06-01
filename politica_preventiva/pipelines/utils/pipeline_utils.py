#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
Utilities used throughout SEDESOL pipeline
"""
import os
import string
import datetime
import psycopg2
import numpy as np
import pandas as pd
import unicodedata
import pandas as pn
import numpy as np
import luigi
import luigi.postgres
from luigi import configuration
from luigi import six
from itertools import product
import boto3

def s3_to_pandas(Bucket,Key,sep="|"):
    """
    Downloads csv from s3 bucket into a pandas Dataframe
    Assumes aws keys as environment variables
    """
    s3 = boto3.client('s3')
    obj = s3.get_object(Bucket=Bucket,Key=Key)

    return pd.read_csv(obj['Body'],sep=sep)


def parse_cfg_list(string):
    """
    Parse string from cfg into a list
    """
    string = string.split(",")
    return [m.strip() for m in string]

def extra_parameters(pipeline, parameters, end_date):
    """
    Create 'extra' argument to pass to pipelines. 
    If 'start_date' is an extra parameter, then it must be the first 
    element in 'parameters'
    Arguments: 
        pipeline: name of pipeline 
        parameters: list containing extra parameters
        end_date: only necessary if using an extra parameter.
    """

    if (len(parameters) == 1 and len(parameters[0]) > 0):

        parsed = parse_cfg_list(configuration.get_config().get(pipeline, parameters[0]))
        if 'start_date' in parameters:
            if end_date:
                dates = date_ranges(parsed[0], end_date)
            else:
                today = datetime.date.today()
                end_date = str(today.year) + "-"+ str(today.month)
                dates = date_ranges(parsed[0], end_date)
            extra = ['']
        else:
            extra = parsed

    elif len(parameters) == 2:
        p1 = parse_cfg_list(configuration.get_config().get(pipeline, parameters[0]))
        p2 = parse_cfg_list(configuration.get_config().get(pipeline, parameters[1]))
        if 'start_date' in parameters:
            if end_date:
                dates = date_ranges(p1[0], end_date)
            else:
                today = datetime.date.today()
                end_date = str(today.year) + "-"+ str(today.month)
                dates = date_ranges(p1[0], end_date)
            extra = p2
        else:
            extra = [v1 + '--' + v2 for v1, v2 in product(p1, p2)]
            dates = [end_date if end_date else '']
    else:
        extra = ['']
        dates = [end_date if end_date else '']
    return [dates, extra]

def date_ranges(start_date, end_date):
    """
    Creates date ranges and returns them in a list. 
    Args:
        (start_date): a string, either YYYY or YYYY-MM
        (end_date): a string, probably a year_month var, YYYY-MM
    Returns:
        (dates): list containg date ranges. Either a list of years or a list of year-monthts
    """
    if '-' in start_date:
        start_year = int(start_date.split('-')[0])
        start_month = int(start_date.split('-')[1])
        end_year = int(end_date.split('-')[0])
        end_month = int(end_date.split('-')[1])
        years = list(range(start_year, end_year + 1))
        months = list(range(1, 13))

        dates = [str(year) + '-' + str(month) for year, month in product(years, months) if (month < end_month or year < end_year) and (month >= start_month or year > start_year)]
    else:
        end_year = int(end_date.split('-')[0])
        dates = [int(year) for year in range(int(start_date), end_year + 1)]

    return dates

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

def lower(df,column):
    df[column] = df[column].map(lambda x: x if type(x)!=str else x.lower())
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
    except NameError: # unicode is a default on python 3
        pass
    text = unicodedata.normalize('NFD', text)
    text = text.encode('ascii', 'ignore')
    text = text.decode("utf-8")
    return str(text)

def cve_loc_construct(cve_ent,cve_mun,cve_loc):
    try:
        cve_ent=str(int(cve_ent)).zfill(2)
        cve_mun=str(int(cve_mun)).zfill(3)
        cve_loc=str(int(cve_loc)).zfill(4)
        cve_locc=cve_ent + cve_mun + cve_loc
        cve_mun=cve_ent + cve_mun

    except:
        cve_locc = ""
    return  pn.Series({'cve_ent':cve_ent,'cve_mun':cve_mun,'cve_locc':cve_locc}) 
