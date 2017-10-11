# coding: utf-8

import re
import os
import ast
import luigi
import psycopg2
import boto3
import random
import sqlalchemy
import tempfile
import glob
import datetime
import subprocess
import pandas as pn

from luigi import six
from os.path import join, dirname
from luigi import configuration
from luigi.contrib import postgres
from luigi.s3 import S3Target, S3Client
from dotenv import load_dotenv,find_dotenv
from luigi.contrib.postgres import PostgresTarget, PostgresQuery

from politica_preventiva.pipelines.utils.pg_sedesol import parse_cfg_string, download_dir
from politica_preventiva.pipelines.tasks.pipeline_tasks import DockerTask
from politica_preventiva.pipelines.utils.pipeline_utils import parse_cfg_list, \
    extras, dates_list, get_extra_str, s3_to_pandas
from politica_preventiva.pipelines.utils import s3_utils

# Variables de ambiente
load_dotenv(find_dotenv())

# logger
logging_conf = configuration.get_config().get("core", "logging_conf_file")
logging.config.fileConfig(logging_conf)
logger = logging.getLogger("dpa-sedesol")

# AWS
aws_access_key_id = os.environ.get('AWS_ACCESS_KEY_ID')
aws_secret_access_key = os.environ.get('AWS_SECRET_ACCESS_KEY')

class ETLPipeline(luigi.WrapperTask):

    current_date = luigi.DateParameter()
    # List all pipelines to run
    pipelines = parse_cfg_list(conf.get("ClassicIngest", "pipelines"))

    def requires(self):
        pdb.set_trace()
        logger.info('Running the following pipelines: {0}'.format(self.pipelines))
        # loop through pipeline tasks
        return [CreateSemanticDB(current_date=self.current_date,
                                   pipeline_task=pipeline)
        for pipeline in self.pipelines]


class CreateSemanticDB(postgres.PostgresQuery):
    current_date = luigi.DateParameter()
    pipeline_task = luigi.Parameter()
    client = S3Client()
    actualizacion = datetime.datetime.now()

    common_path = luigi.Parameter('DEFAULT')
    local_path = luigi.Parameter('DEFAULT')  # path where csv is located
    historical = luigi.Parameter('DEFAULT')


    @property
    def dates(self):
        dates, self.suffix =  final_dates(self.historical, self.pipeline_task, 
                                          self.current_date)
        return dates



class CreateCleanDB(postgres.PostgresQuery):

	"""
	"""

	year_month = luigi.Parameter()
	sql_scripts = luigi.Parameter('EtlPipeline')
	database = os.environ.get("PGDATABASE_COMPRANET")
	user = os.environ.get("POSTGRES_USER_COMPRANET")
	password = os.environ.get("POSTGRES_PASSWORD_COMPRANET")
	host = os.environ.get("PGHOST_COMPRANET")

	@property
	def update_id(self):
		num = str(random.randint(0,100000))
		return num 

	@property
	def table(self):
		return "clean.diccionario_dependencias" 

	@property
	def query(self):
		sqlfile = open('./etl/sql_scripts/clean.sql', 'r')
		query = sqlfile.read()
		return query 

	def output(self):

		return luigi.postgres.PostgresTarget(host=self.host,database=self.database,user=self.user,
			password=self.password,table=self.table,update_id=self.update_id)
