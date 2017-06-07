# coding: utf-8
# Run as: luigid & PYTHONPATH='.' python politica_preventiva.py
# RunPipelines --workers 3

import os
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

from politica_preventiva.pipelines.ingest.ingest_orchestra import UpdateDB, Concatenation
from politica_preventiva.pipelines.etl.etl_orchestra import ETL
from politica_preventiva.pipelines.utils.pipeline_utils import parse_cfg_list, extra_parameters, historical_dates
import pdb

logger = logging.getLogger("dpa-sedesol.plataforma_preventiva")

# Variables de ambiente
load_dotenv(find_dotenv())

# AWS
aws_access_key_id = os.environ.get('AWS_ACCESS_KEY_ID')
aws_secret_access_key = os.environ.get('AWS_SECRET_ACCESS_KEY')

class RunPipelines(luigi.WrapperTask):

    """
    Task principal para el pipeline 
    """

    # Pipeline corre mensualmente
    # start_year_month= el pipe de adolfo incluye un start month -> ver rita
    current_date = luigi.DateParameter(default=datetime.date(2017, 5, 4)) # datetime.date.today()

    def requires(self):

        yield Ingestpipeline(self.current_date)
        #yield EtlPipeline(self.year_month)


class Ingestpipeline(luigi.WrapperTask):

    """
    This wrapper task executes ingest pipeline
    It expects a list specifying which ingest pipelines to run
    """

    # ToDo() Checar la temporalidad de la ingesta: (e.g. si es mensual y toca
    # correr)

    current_date = luigi.DateParameter()
    conf = configuration.get_config()

    # List all pipelines to run
    pipelines = parse_cfg_list(conf.get("Ingestpipeline", "pipelines"))

    def requires(self):
        # loop through pipeline tasks
        yield [UpdateDB(current_date=self.current_date,
                             pipeline_task=pipeline) for pipeline in self.pipelines]


class EtlPipeline(luigi.WrapperTask):

    """
        Este wrapper ejecuta el ETL de cada pipeline-task.

    """

    year_month = luigi.Parameter()

    def requires(self):

        return IngestPipeline(self.year_month)

    def run(self):

        yield ETL(year_month=self.year_month)


# class ModelPipeline(luigi.WrapperTask):

#     """
#         Este wrapper ejecuta los modelos
#     """

#     year_month = luigi.Parameter()
#     conf = configuration.get_config()

#     def requires(self):

#         yield MissingClassifier(year_month=self.year_month)
#         yield SetNeo4J(year_month=self.year_month)
#         yield PredictiveModel(year_month=self.year_month)



if __name__ == "__main__":
    luigi.run()
