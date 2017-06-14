# coding: utf-8
# Run as: luigid & PYTHONPATH='.' python politica_preventiva.py
# RunPipelines --workers 3

import os
import ast
import datetime
import logging
import boto3
import luigi
import luigi.s3
import multiprocessing
from dotenv import load_dotenv
from os.path import join, dirname
from luigi.s3 import S3Target, S3Client
from luigi import configuration
from joblib import Parallel, delayed
from itertools import product
from dotenv import load_dotenv,find_dotenv

from politica_preventiva.pipelines.ingest.ingest_orchestra import IngestPipeline
#from politica_preventiva.pipelines.etl.etl_orchestra import ETLPipeline
#from politica_preventiva.pipelines.model.model_orchestra import ModelPipeline
from politica_preventiva.pipelines.utils.pipeline_utils import parse_cfg_list, extra_parameters, historical_dates
import pdb

logger = logging.getLogger("dpa-sedesol.plataforma_preventiva")

# Variables de ambiente
load_dotenv(find_dotenv())

# Load Postgres Schemas
temp = open('./pipelines/common/pg_raw_schemas.txt').read()
schemas = ast.literal_eval(temp)
open('./pipelines/common/pg_raw_schemas.txt').close()

# RDS
database = os.environ.get("PGDATABASE")
user = os.environ.get("POSTGRES_USER")
password = os.environ.get("POSTGRES_PASSWORD")
host = os.environ.get("PGHOST")

# AWS
aws_access_key_id = os.environ.get('AWS_ACCESS_KEY_ID')
aws_secret_access_key = os.environ.get('AWS_SECRET_ACCESS_KEY')
PLACES_API_KEY =  os.environ.get('PLACES_API_KEY')


class RunPipelines(luigi.WrapperTask):

    """
    Task principal para el pipeline 
    """
    # start_year_month= el pipe de adolfo incluye un start month -> ver rita
    #luigi.DateParameter(default=datetime.date(2017, 5, 4)) 

    current_date = datetime.date.today()

    def requires(self):

        yield IngestPipeline(current_date=self.current_date)
        #yield ETLPipeline(self.current_date)
        #yield ModelPipeline(self.current_date)

if __name__ == "__main__":
    luigi.run()