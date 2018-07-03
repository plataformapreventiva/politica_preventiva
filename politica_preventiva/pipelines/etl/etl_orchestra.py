#!/usr/bin/env python
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

from politica_preventiva.pipelines.utils.pg_tools import PGWrangler
from politica_preventiva.tasks.pipeline_task import DockerTask, PgRTask
from politica_preventiva.pipelines.ingest.ingest_orchestra import UpdateLineage
from politica_preventiva.pipelines.ingest.tools.ingest_utils import parse_cfg_list,\
    extras, dates_list, get_extra_str, s3_to_pandas, final_dates
from politica_preventiva.pipelines.utils import s3_utils

# Env Setup
load_dotenv(find_dotenv())

# Logger & Config
configuration.LuigiConfigParser.add_config_path('/pipelines/configs/luigi_models.cfg')
conf = configuration.get_config()

logging_conf = configuration.get_config().get("core", "logging_conf_file")
logging.config.fileConfig(logging_conf)
logger = logging.getLogger("dpa-sedesol")

# AWS
aws_access_key_id = os.environ.get('AWS_ACCESS_KEY_ID')
aws_secret_access_key = os.environ.get('AWS_SECRET_ACCESS_KEY')


class ETLPipeline(luigi.WrapperTask):

    current_date = luigi.DateParameter()
    pipelines = luigi.parameter.ListParameter()
    ptask = luigi.Parameter()
    client = S3Client()
    common_path = luigi.Parameter('DEFAULT')
    local_path = luigi.Parameter('DEFAULT')  # path where csv is located
    historical = luigi.Parameter('DEFAULT')

    def requires(self):
        if self.ptask!='auto':
            self.pipelines = (self.ptask,)

        logger.info('Running the following pipelines: {0}'.format(self.pipelines))
        # loop through pipeline tasks and data dates
        set_pipelines = [(pipeline_task, final_dates(self.historical,
                                                     pipeline_task,
                                                     self.current_date)) for
                         pipeline_task in self.pipelines]
        return [UpdateTidyDB(current_date=self.current_date,
                             pipeline_task=pipeline[0],
                             data_date=dates,
                             suffix=pipeline[1][1])
                for pipeline in set_pipelines for dates in pipeline[1][0]]


class UpdateTidyDB(PgRTask):

    """
    This Task runs the tidy script in tidy folder for
    the pipeline_task, if it doesn't exists then it runs
    the no_tidy.R script from the same folder.
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
        tidy_file = self.tidy_scripts +\
                self.pipeline_task + '.R'

        if os.path.isfile(tidy_file):
            pass
        else:
            tidy_file = self.tidy_scripts + 'no_tidy.R'

        command_list = ['Rscript', tidy_file,
                        '--data_date', self.data_date,
                        '--database', self.database,
                        '--user', self.user,
                        '--password', "'{}'".format(self.password),
                        '--host', self.host,
                        '--pipeline', self.pipeline_task]
        cmd = " ".join(command_list)
        return cmd

    @property
    def update_id(self):
        return str(self.pipeline_task) + '_' + str(self.data_date) +\
                str(self.suffix) + '_tidy'

    @property
    def table(self):
        return "tidy." + self.pipeline_task

    def requires(self):
        return UpdateCleanDB(current_date=self.current_date,
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


class UpdateCleanDB(postgres.PostgresQuery):
    """
    This Task runs the Clean script in clean folder for
    the pipeline_task, if it doesn't exists then it only updates
    with the last raw table
    """

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

        try:
            sqlfile = open(path, 'r')
            query = sqlfile.read()

        except:
            query = """DROP TABLE IF EXISTS {0};
            CREATE TABLE {0} AS (SELECT * FROM
                                 raw.{1});""".format(self.table,
                                                     self.pipeline_task)
        return query

    def requires(self):
        return UpdateLineage(current_date=self.current_date,
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
