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
from politica_preventiva.pipelines.utils.pipeline_utils import parse_cfg_list,\
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
    pipelines = luigi.Parameter()
    ptask = luigi.Parameter()
    client = S3Client()
    common_path = luigi.Parameter('DEFAULT')
    local_path = luigi.Parameter('DEFAULT')  # path where csv is located

    def requires(self):
        if self.ptask!='auto':
            self.pipelines = (self.ptask,)

        logger.info('Running the following pipelines: {0}'.format(self.pipelines))
        # loop through pipeline tasks and data dates
        set_pipelines = [(pipeline_task, final_dates(pipeline_task,
                                                     self.current_date)) for
                         pipeline_task in self.pipelines]
        return [UpdateCleanDB(current_date=self.current_date,
                             pipeline_task=pipeline[0],
                             data_date=dates,
                             suffix=pipeline[1][1])
                for pipeline in set_pipelines for dates in pipeline[1][0]]

class UpdateCleanDB(PgRTask):

    """
    This Task runs the Clean script in clean folder for
    the pipeline_task, using a wrapper script in order to do clean and recode
    in one process. If there is no clean script to be run, it updates
    with the last (recoded) raw table
    """

    current_date = luigi.DateParameter()
    pipeline_task = luigi.Parameter()
    client = S3Client()
    data_date = luigi.Parameter()
    suffix = luigi.Parameter()
    clean_scripts = luigi.Parameter()
    clean_wrapper = luigi.Parameter()

    # RDS
    database = os.environ.get("PGDATABASE")
    user = os.environ.get("POSTGRES_USER")
    password = os.environ.get("POSTGRES_PASSWORD")
    host = os.environ.get("PGHOST")
    port = os.environ.get("PGPORT")

    @property
    def cmd(self):
        command_list = ['Rscript', self.clean_wrapper,
                        '--data_date', self.data_date,
                        '--database', self.database,
                        '--port', self.port,
                        '--user', self.user,
                        '--password', "'{}'".format(self.password),
                        '--host', self.host,
                        '--pipeline_task', self.pipeline_task,
                        '--scripts_dir', self.clean_scripts]
        cmd = " ".join(command_list)
        return cmd

    @property
    def update_id(self):
        return str(self.pipeline_task) + str(self.data_date) +\
               str(self.suffix) + '_clean'

    @property
    def table(self):
        return "clean." + self.pipeline_task

    def requires(self):
        return UpdateLineage(current_date=self.current_date,
                             pipeline_task=self.pipeline_task,
                             data_date=self.data_date,
                             suffix=self.suffix)

    def output(self):
        return PostgresTarget(host=self.host,
                              database=self.database,
                              port=self.port,
                              user=self.user,
                              password=self.password,
                              table=self.table,
                              update_id=self.update_id)
