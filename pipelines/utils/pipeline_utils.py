"""
Utilities used throughout Compranet pipeline
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


#LOGGING_CONF = configuration.get_config().get("core", "logging_conf_file")
#logging.config.fileConfig(LOGGING_CONF)
#logger = logging.getLogger("compranet.pipeline")


def parse_cfg_list(string):
    """
    Parse string from cfg into a list
    """
    string = string.split(",")
    return [m.strip() for m in string]

def extra_parameters(pipeline, parameters):
    """
    Create 'extra' argument to pass to pipelines
    Arguments: 
        pipeline: name of pipeline 
        parameters: list containing extra parameters
    """
    if len(parameters) == 1 & len(parameters[0]) > 0:
        extra = parse_cfg_list(configuration.get_config().get(pipeline, parameters[0]))
    elif len(parameters) == 2:
        p1 = parse_cfg_list(configuration.get_config().get(pipeline, parameters[0]))
        p2 = parse_cfg_list(configuration.get_config().get(pipeline, parameters[1]))
        extra = [v1 + '--' + v2 for v1, v2 in product(p1, p2)]
    else:
        extra = ['']
    return extra 


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