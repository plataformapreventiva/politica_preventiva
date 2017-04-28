"""
Utilities used throughout Compranet pipeline
"""
import os
import string
import datetime
import psycopg2
import numpy as np
import pandas as pd
#import json
#import re
#import hashlib
#import logging
#import logging.config
#from pandas.compat import range, lzip, map

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