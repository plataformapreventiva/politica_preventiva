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
from politica_preventiva.pipelines.ingest.tools.ingest_utils import parse_cfg_list,\
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
        return UpdateTidyDB(current_date=self.current_date,
                            pipeline_task=self.pipeline_task,
                            data_date=self.data_date,
                            suffix=self.suffix)

    def output(self):
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

    # RDS
    database = os.environ.get("PGDATABASE")
    user = os.environ.get("POSTGRES_USER")
    password = os.environ.get("POSTGRES_PASSWORD")
    host = os.environ.get("PGHOST")

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
        return cmd

    @property
    def update_id(self):
        print(self.cmd)
        return str(self.pipeline_task) + '_' + str(self.data_date) +\
               str(self.suffix) + '_tidy'

    @property
    def table(self):
        return "tidy." + self.pipeline_task

    def output(self):
        return PostgresTarget(host=self.host,
                              database=self.database,
                              user=self.user,
                              password=self.password,
                              table=self.table,
                              update_id=self.update_id)

