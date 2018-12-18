#!/usr/bin/env python

# coding: utf-8

import datetime
import luigi
import os
import random
import subprocess
import logging
import pandas as pd
import pdb
import psycopg2
import yaml

from boto3 import client, resource
from sqlalchemy import create_engine

from os.path import join, dirname
from dotenv import load_dotenv, find_dotenv

from luigi import six
from luigi import configuration
from luigi.contrib import postgres
from luigi.s3 import S3Target, S3Client
from luigi.contrib.postgres import PostgresTarget, PostgresQuery

from politica_preventiva.pipelines.utils.pg_sedesol import parse_cfg_string,\
        download_dir
from politica_preventiva.tasks.pipeline_task import DockerTask, PgRTask
from politica_preventiva.pipelines.utils.pipeline_utils import parse_cfg_list,\
    extras, dates_list, get_extra_str, s3_to_pandas, final_dates
from politica_preventiva.pipelines.utils import s3_utils
from politica_preventiva.pipelines.tests import pipeline_tests
from politica_preventiva.pipelines.etl.etl_orchestra import ETLPipeline
from politica_preventiva.pipelines.features.tools.pipeline_tools import get_features_dates
#from politica_preventiva.pipelines.models.models_orchestra import ModelsPipeline

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
            self.pipelines = (self.ptask,)

        logger.info('Luigi is running the Features Pipeline on the date: {0}'.format(
                    self.current_date))
        logger.info('Running the following pipelines: {0}'.\
                    format(self.pipelines))
        set_pipelines = [(features_task, get_features_dates(features_task,
                                                            self.current_date)) for
                          features_task in self.pipelines]
        return [UpdateFeaturesDictionary(features_task=pipeline[0],
                                 current_date=self.current_date,
                                 data_date = dates,
                                 suffix=pipeline[1][1])
                for pipeline in set_pipelines for dates in pipeline[1][0]]

class UpdateFeaturesDictionary(postgres.CopyToTable):

    """
    Updates Postgres DataBase for Dictionary with new datadate ingested data.
    Assumes that the ditcionary of the features_task is defined
    """
    current_date = luigi.DateParameter()
    features_task = luigi.Parameter()
    client = S3Client()
    data_date = luigi.Parameter()
    suffix = luigi.Parameter()

    common_path = luigi.Parameter()
    common_bucket = luigi.Parameter()
    common_key = luigi.Parameter()

    # AWS RDS
    database = os.environ.get("PGDATABASE")
    user = os.environ.get("POSTGRES_USER")
    password = os.environ.get("POSTGRES_PASSWORD")
    host = os.environ.get("PGHOST")

    @property
    def update_id(self):
        return str(self.features_task) + str(self.data_date) +\
               str(self.suffix) + '_features_dic'

    @property
    def table(self):
        return "features." + self.features_task + "_dic"

    @property
    def dict_path(self):
        return self.common_path + self.features_task + '_dic.csv'

    @property
    def columns(self):
        return [('id', 'TEXT'),
                ('nombre', 'TEXT'),
                ('tipo','TEXT'),
                ('fuente','TEXT'),
                ('metadata', 'JSONB'),
                ('actualizacion_sedesol', 'TIMESTAMP'),
                ('data_date', 'TEXT')]

    @property
    def table_header(self):
        engine = create_engine('postgresql://{0}:{1}@{2}:{3}/{4}'.\
                                format(os.getenv('POSTGRES_USER'),
                                       os.getenv('POSTGRES_PASSWORD'),
                                       os.getenv('PGHOST'),
                                       os.getenv('PGPORT'),
                                       os.getenv('PGDATABASE')))
        header_query = ("select column_name from information_schema.columns "
                        "where table_schema = 'features' "
                        "and table_name='{}' "
                        "and column_name <> 'actualizacion_sedesol' "
                        "and column_name <> 'data_date'".format(self.features_task))
        connection = engine.connect()
        query_result = connection.execute(header_query)
        table_columns = [x for x in query_result]
        connection.close()
        if not table_columns:
            raise Exception('Could not find table features.{},\
                    please check the execution status of your features script'.\
                            format(self.features_task))
        return [x[0] for x in table_columns]

    def rows(self):
        dict_header = [x[0] for x in self.columns]
        logging.info('Trying to update the dictionary for '+\
                     'the features task {}'.format(self.features_task))
        dictionary=pipeline_tests.dictionary_test(task=self.features_task,
                                                  dict_path=self.dict_path,
                                                  allow_joins=True,
                                                  table_header=self.table_header,
                                                  dict_header=dict_header,
                                                  current_date=self.current_date,
                                                  data_date=self.data_date,
                                                  suffix=self.suffix,
                                                  common_bucket=self.common_bucket,
                                                  common_key=self.common_key)
        return [tuple(x) for x in dictionary.to_records(index=False)]

    def requires (self):
        return UpdateFeaturesDB(features_task=self.features_task,
                                current_date=self.current_date,
                                data_date=self.data_date,
                                suffix=self.suffix)

    def output(self):
        return postgres.PostgresTarget(host=self.host,
                                       database=self.database,
                                       user=self.user,
                                       password=self.password,
                                       table=self.table,
                                       update_id=self.update_id)

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
            raise Exception("Feature script is not defined")

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
        dep = composition[self.features_task]
        if dep!=None:
            dep_types = [dt for dt in dep.keys()]
        else:
            return


        for dt in dep_types:
            if 'features_dependencies' in dep_types:
                features_tables = composition[self.features_task]['features_dependencies']
                yield [FeaturesPipeline(current_date=self.current_date,
                                        ptask=pipeline_task) for pipeline_task in features_tables]

            if 'clean_dependencies' in dep_types:
                clean_tables = composition[self.features_task]['clean_dependencies']
                yield [ETLPipeline(current_date=self.current_date,
                                   ptask=pipeline_task) for pipeline_task in clean_tables]

            if 'model_dependencies' in dep_types:
                models_tables = composition[self.features_task]['models_dependencies']
                yield [ModelsPipeline(current_date=self.current_date,
                                      ptask=pipeline_task) for pipeline_task in models_tables]

    def output(self):
        return PostgresTarget(host=self.host,
                              database=self.database,
                              user=self.user,
                              password=self.password,
                              table=self.table,
                              update_id=self.update_id)

