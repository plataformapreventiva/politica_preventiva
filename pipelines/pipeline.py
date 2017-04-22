# coding: utf-8
# Run as: PYTHONPATH='.' luigi --module pipeline Ingestpipelines --local-scheduler

import os
from os.path import join, dirname
import logging
from luigi import configuration
from dotenv import load_dotenv
import datetime
import logging
from dotenv import load_dotenv
import boto3
import luigi
import luigi.s3
from luigi.s3 import S3Target, S3Client
from luigi import configuration

#import sqlalchemy
#import dummy.config_ini
#import os
#import subprocess
#import pandas as pd
#import csv
#import datetime

logger = logging.getLogger("dpa-sedesol.dummy")
from utils.pipeline_utils import parse_cfg_list
from ingest.ingest_orchestra import bash_ingestion_s3

## Variables de ambiente
path = os.path.abspath('__file__' + "/../../config/")
dotenv_path = join(path, '.env')
load_dotenv(dotenv_path)

## Obtenemos las llaves de AWS
aws_access_key_id =  os.environ.get('AWS_ACCESS_KEY_ID')
aws_secret_access_key = os.environ.get('AWS_SECRET_ACCESS_KEY')

aws_access_key_id="AKIAIRLNSTJUPRAPLISA"
aws_secret_access_key="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"


class RunPipelines(luigi.WrapperTask):
    """
    Task principal para el pipeline 
    """
    # Pipeline corre mensualmente
    #start_year_month= el pipe de adolfo incluye un start month -> ver rita
    today = datetime.date.today()
    year_month = str(today.year) + "-"+ str(today.month)

    def requires(self):
        yield Ingestpipeline(self.year_month)


class Ingestpipeline(luigi.WrapperTask):
    """
    This wrapper task executes ingest pipeline
    It expects a list specifying which ingest pipelines to run
    """
    year_month = luigi.Parameter()
    conf = configuration.get_config()
    bash_pipelines = parse_cfg_list(conf.get("Ingestpipeline", "bash_pipelines"))
    #python_pipelines = parse_cfg_list(conf.get("Ingestpipeline", "python_pipelines"))

    def requires(self):
        for pipeline in self.bash_pipelines:
            yield bash_ingestion_s3(pipeline_task=pipeline, year_month=self.year_month)
        #for pipeline in self.docker_pipelines:
        #    yield docker_ingestion_s3(pipeline)


if __name__ == "__main__":
    luigi.run()