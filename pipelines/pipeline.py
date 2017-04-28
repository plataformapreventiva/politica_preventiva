# coding: utf-8
# Run as: luigid & PYTHONPATH='.' python pipeline.py  RunPipelines --workers 3

#-local_ingest
#-local_to_s3
#-update_s3
#-s3_to_postgres

import os
import datetime
from os.path import join, dirname
import logging
from dotenv import load_dotenv
import boto3
import luigi
import luigi.s3
from luigi.s3 import S3Target, S3Client
from luigi import configuration
from joblib import Parallel, delayed
import multiprocessing
from itertools import product

logger = logging.getLogger("dpa-sedesol.dummy")
from utils.pipeline_utils import parse_cfg_list, extra_parameters
from ingest.ingest_orchestra import classic_ingest, local_to_s3

## Variables de ambiente
path = os.path.abspath('__file__' + "/../../config/")
dotenv_path = join(path, '.env')
load_dotenv(dotenv_path)

## Obtenemos las llaves de AWS
aws_access_key_id =  os.environ.get('AWS_ACCESS_KEY_ID')
aws_secret_access_key = os.environ.get('AWS_SECRET_ACCESS_KEY')

class RunPipelines(luigi.WrapperTask):
    """
    Task principal para el pipeline 
    """
    # Pipeline corre mensualmente
    #start_year_month= el pipe de adolfo incluye un start month -> ver rita
    today = datetime.date.today()
    year_month = str(today.year) + "-"+ str(today.month)
    year_month_day = year_month + "-" + str(today.day)

    def requires(self):
        yield Ingestpipeline(self.year_month)


class Ingestpipeline(luigi.WrapperTask):
    """
    This wrapper task executes ingest pipeline
    It expects a list specifying which ingest pipelines to run
    """
    #ToDo() Checar la temporalidad de la ingesta: (e.g. si es mensual y toca correr)

    year_month = luigi.Parameter()
    conf = configuration.get_config()
    
    # List all pipelines to run 
    pipelines = parse_cfg_list(conf.get("Ingestpipeline", "pipelines"))
    # Get number of cores in which to run pipeline 
    num_cores = multiprocessing.cpu_count()

    def requires(self):
        params = {pipeline: parse_cfg_list(configuration.get_config().get(pipeline, "extra_parameters")) for pipeline in self.pipelines}
        extra = {pipeline: extra_parameters(pipeline, params[pipeline]) for pipeline in self.pipelines}
        yield Parallel(n_jobs=self.num_cores)(delayed(local_to_s3)(pipeline_task=pipeline, year_month=self.year_month, extra=extra_p) for 
        pipeline in self.pipelines for extra_p in extra[pipeline])
        

if __name__ == "__main__":
    luigi.run()
