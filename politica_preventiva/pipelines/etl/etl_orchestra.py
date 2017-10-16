# coding: utf-8

import datetime
import luigi
import os
import random
import subprocess
import logging
import pdb

from luigi import six
from os.path import join, dirname
from luigi import configuration
from luigi.contrib import postgres
from luigi.s3 import S3Target, S3Client
from dotenv import load_dotenv, find_dotenv
from luigi.contrib.postgres import PostgresTarget, PostgresQuery

from politica_preventiva.pipelines.utils.pg_sedesol import parse_cfg_string,\
        download_dir
from politica_preventiva.tasks.pipeline_task import DockerTask, RTask
from politica_preventiva.pipelines.utils.pipeline_utils import parse_cfg_list,\
    extras, dates_list, get_extra_str, s3_to_pandas, final_dates
from politica_preventiva.pipelines.utils import s3_utils

# Variables de ambiente
load_dotenv(find_dotenv())

# logger
conf = configuration.get_config()
logging_conf = configuration.get_config().get("core", "logging_conf_file")

logging.config.fileConfig(logging_conf)
logger = logging.getLogger("dpa-sedesol")

# AWS
aws_access_key_id = os.environ.get('AWS_ACCESS_KEY_ID')
aws_secret_access_key = os.environ.get('AWS_SECRET_ACCESS_KEY')


class ETLPipeline(luigi.WrapperTask):

    current_date = luigi.DateParameter()
    pipelines = parse_cfg_list(conf.get("ClassicIngest", "pipelines"))
    client = S3Client()
    common_path = luigi.Parameter('DEFAULT')
    local_path = luigi.Parameter('DEFAULT')  # path where csv is located
    historical = luigi.Parameter('DEFAULT')

    def requires(self):
        logger.info('Running the following pipelines: {0}'.format(self.pipelines))
        # loop through pipeline tasks and data dates
        set_pipelines = [(pipeline_task, final_dates(self.historical,
                                                     pipeline_task,
                                                     self.current_date)) for 
                         pipeline_task in self.pipelines]
 
        return [UpdateCleanDB(current_date=self.current_date,
                              pipeline_task=pipeline[0],
                              data_date=dates,
                              suffix=pipeline[1][1])
                for pipeline in set_pipelines for dates in pipeline[1][0]]


class UpdateCleanDB(postgres.PostgresQuery):

    current_date = luigi.DateParameter()
    pipeline_task = luigi.Parameter()
    client = S3Client()
    data_date = luigi.Parameter()
    suffix = luigi.Parameter()

    clean_scripts = luigi.Parameter()
    # RDS
    database = os.environ.get("PGDATABASE")
    user = os.environ.get("POSTGRES_USER")
    password = os.environ.get("POSTGRES_PASSWORD")
    host = os.environ.get("PGHOST")

    @property
    def update_id(self):
        return str(self.pipeline_task) + str(self.data_date) +\
               str(self.suffix) + '_clean'

    @property
    def table(self):
        return "clean." + self.pipeline_task

    @property
    def query(self):
        # Read sql command
        path = self.clean_scripts + self.pipeline_task + '.sql'
        sqlfile = open(path, 'r')
        query = sqlfile.read()
        return query

    def requires(self):
        pdb.set_trace()
        UpdateTidyDB(current_date=self.current_date,
                     pipeline_task=self.pipeline_task,
                     data_date=self.data_date,
                     suffix=self.suffix)

    def output(self):
        pdb.set_trace()
        return PostgresTarget(host=self.host,
                              database=self.database,
                              user=self.user,
                              password=self.password,
                              table=self.table,
                              update_id=self.update_id)


class UpdateTidyDB(RTask):
    """
    """

    current_date = luigi.DateParameter()
    pipeline_task = luigi.Parameter()
    client = S3Client()
    data_date = luigi.Parameter()
    suffix = luigi.Parameter()
    tidy_scripts = luigi.Parameter()

    database = os.environ.get("PGDATABASE_COMPRANET")
    user = os.environ.get("POSTGRES_USER_COMPRANET")
    password = os.environ.get("POSTGRES_PASSWORD_COMPRANET")
    host = os.environ.get("PGHOST_COMPRANET")

    @property
    def update_id(self):
        return str(self.pipeline_task) + str(self.data_date) +\
               str(self.suffix) + '_tidy'

    @property
    def table(self):
        return "tidy.diccionario_dependencias"

    @property
    def cmd(self):
        command_list = ['Rscript', self.tidy_scripts +\
                        self.pipeline_task + '.R',
                        '--data_date', self.data_date,
                        '--database', self.database,
                        '--user', self.user,
                        '--password', self.password,
                        '--host', self.host]
        cmd = " ".join(command_list)

        return subprocess.call([cmd], shell=True)

    def output(self):
        return PostgresTarget(host=self.host,
                              database=self.database,
                              user=self.user,
                              password=self.password,
                              table=self.table,
                              update_id=self.update_id)

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
