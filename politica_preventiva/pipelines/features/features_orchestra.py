#!/usr/bin/env python
# coding: utf-8

import datetime
import luigi
import os
import random
import subprocess
import logging
import pdb
import yaml

from luigi import six
from os.path import join, dirname
from luigi import configuration
from luigi.contrib import postgres
from luigi.s3 import S3Target, S3Client
from dotenv import load_dotenv, find_dotenv
from luigi.contrib.postgres import PostgresTarget, PostgresQuery

from politica_preventiva.pipelines.utils.pg_sedesol import parse_cfg_string,\
        download_dir
from politica_preventiva.tasks.pipeline_task import DockerTask, PgRTask
from politica_preventiva.pipelines.ingest.tools.ingest_utils import parse_cfg_list,\
    extras, dates_list, get_extra_str, s3_to_pandas, final_dates
from politica_preventiva.pipelines.utils import s3_utils
from politica_preventiva.pipelines.etl.etl_orchestra import ETLPipeline

# Environment Setup
load_dotenv(find_dotenv())

# Logger and Config
conf = configuration.get_config()
logging_conf = configuration.get_config().get("core", "logging_conf_file")

logging.config.fileConfig(logging_conf)
logger = logging.getLogger("dpa-sedesol")

# AWS
aws_access_key_id = os.environ.get('AWS_ACCESS_KEY_ID')
aws_secret_access_key = os.environ.get('AWS_SECRET_ACCESS_KEY')

# Semantic Schema

with open("pipelines/configs/features_dependencies.yaml", "r") as file:
    composition = yaml.load(file)


class FeaturesPipeline(luigi.WrapperTask):

    """
    This Task programs the features creation for each missing data date, for a
    given set of pipelines.
    """
    pipelines = luigi.parameter.ListParameter()
    current_date = luigi.DateParameter()
    client = S3Client()
    ptask = luigi.Parameter()

    def requires(self):
        if self.ptask!='auto':
            self.pipelines = [self.ptask]

        logger.info('Luigi is running the Features Pipeline on the date: {0}'.format(
                    self.current_date))
        logger.info('Running the following pipelines: {0}'.\
                    format(self.pipelines))

        set_pipelines = [(pipeline_task, final_dates(pipeline_task,
                                                     self.current_date)) for
                         pipeline_task in self.pipelines]
        return [UpdateFeaturesDB(features_task=pipeline[0],
                                 current_date=self.current_date,
                                 data_date = dates,
                                 suffix=pipeline[1][1])
                for pipeline in set_pipelines for dates in pipeline[1][0]]

class UpdateFeaturesDB(PgRTask):

    """
    This Task runs the feature creation script for a given set of features.
    All the feature creation processes are currently run through R scripts,
    which receive DB connection settings and a data date as input parameters.
    """
    features_task = luigi.Parameter()
    current_date = luigi.DateParameter()
    data_date = luigi.Parameter()
    suffix = luigi.Parameter()
    client = S3Client()
    features_scripts = luigi.Parameter()

    # AWS RDS
    database = os.environ.get("PGDATABASE")
    user = os.environ.get("POSTGRES_USER")
    password = os.environ.get("POSTGRES_PASSWORD")
    host = os.environ.get("PGHOST")

    @property
    def update_id(self):
        return str(self.features_task) + str(self.data_date) +\
                str(self.suffix) + '_features'

    @property
    def table(self):
        return "features." + self.features_task

    @property
    def cmd(self):
        # Read features script
        features_script = self.features_scripts +\
                self.features_task + '.R'

        if not os.path.isfile(features_script):
            return

        command_list = ['Rscript', features_script,
                        '--data_date', self.data_date,
                        '--database', self.database,
                        '--user', self.user,
                        '--password', "'{}'".format(self.password),
                        '--host', self.host]
        cmd = " ".join(command_list)
        return cmd

    def requires(self):
        # Check table dependencies
        dep_types = [dt for dt in composition[self.features_task].keys()]

        for dt in dep_types:
            if 'features_dependencies' in dep_types:
                features_tables = composition[self.features_task]['features_dependencies']
                yield [FeaturesPipeline(current_date=self.current_date,
                    pipelines=[pipeline_task], ptask='auto') for pipeline_task in features_tables]

            if 'clean_dependencies' in dep_types:
                clean_tables = composition[self.features_task]['clean_dependencies']
                yield [ETLPipeline(current_date=self.current_date,
                    pipelines=[pipeline_task], ptask='auto') for pipeline_task in clean_tables]

            if 'model_dependencies' in dep_types:
                models_tables = composition[self.features_task]['models_dependencies']
                yield [ModelsPipeline(current_date=self.current_date,
                    models=[pipeline_task], ptask='auto') for pipeline_task in models_tables]

    def output(self):
        return PostgresTarget(host=self.host,
                              database=self.database,
                              user=self.user,
                              password=self.password,
                              table=self.table,
                              update_id=self.update_id)

