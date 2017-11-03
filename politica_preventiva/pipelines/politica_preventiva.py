# coding: utf-8
# If you Run as: luigid & PYTHONPATH='.' python politica_preventiva.py
# RunPipelines --workers 3

import ast
import boto3
import datetime
import logging
import luigi
import luigi.s3
import os
import pdb

from dotenv import load_dotenv
from os.path import join, dirname
from luigi.s3 import S3Target, S3Client
from luigi import configuration
from joblib import Parallel, delayed
from itertools import product
from dotenv import find_dotenv

from politica_preventiva.pipelines.ingest.ingest_orchestra import\
    IngestPipeline
from politica_preventiva.pipelines.etl.etl_orchestra import ETLPipeline
from politica_preventiva.pipelines.ingest.tools.ingest_utils import\
        parse_cfg_list
from politica_preventiva.pipelines.semantic.semantic_orchestra import\
        SemanticPipeline

# from politica_preventiva.pipelines.model.model_orchestra import ModelPipeline
logging_conf = configuration.get_config().get("core", "logging_conf_file")
logging.config.fileConfig(logging_conf)
logger = logging.getLogger("dpa-sedesol")

# Variables de ambiente
load_dotenv(find_dotenv())

# RDS
database = os.environ.get("PGDATABASE")
user = os.environ.get("POSTGRES_USER")
password = os.environ.get("POSTGRES_PASSWORD")
host = os.environ.get("PGHOST")

# AWS
aws_access_key_id = os.environ.get('AWS_ACCESS_KEY_ID')
aws_secret_access_key = os.environ.get('AWS_SECRET_ACCESS_KEY')
PLACES_API_KEY = os.environ.get('PLACES_API_KEY')


class RunPipelines(luigi.WrapperTask):

    """
    Main Wrapper Task of pipelines
    """
    current_date = datetime.date.today()
    # current_date = luigi.DateParameter(default=datetime.date(2017, 8, 12))
    logger.info('Luigi is running the pipeline on the date: {0}'.format(
        current_date))

    def requires(self):

        return IngestPipeline(current_date=self.current_date)
        # return ETLPipeline(current_date=self.current_date)
        # return SemanticPipeline(current_date=self.current_date)
        # return ModelPipeline(current_date=self.current_date)


if __name__ == "__main__":
    luigi.run()
