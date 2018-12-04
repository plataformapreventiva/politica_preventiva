#!/usr/bin/env python
# coding: utf-8

import datetime
import logging
import luigi
import os
import pandas as pd
import pdb
import random
import subprocess
import yaml

from sqlalchemy import create_engine

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
from politica_preventiva.pipelines.utils.pipeline_utils import parse_cfg_list,\
    extras, dates_list, get_extra_str, s3_to_pandas, final_dates
from politica_preventiva.pipelines.utils import s3_utils
from politica_preventiva.pipelines.etl.etl_orchestra import UpdateCleanDB
from politica_preventiva.pipelines.features.tools.pipeline_tools import
get_feature_dates
from politica_preventica.pipelines.semantic.tools.pipeline_tools import plots_test

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

# Semantic Schema

with open("pipelines/configs/semantic_dependencies.yaml", "r") as file:
    composition = yaml.load(file)


class SemanticPipeline(luigi.WrapperTask):

    semantics = parse_cfg_list(conf.get("SemanticPipeline", "pipelines"))
    current_date = luigi.DateParameter()
    client = S3Client()
    ptask = luigi.Parameter()
    pipelines = luigi.Parameter()

    def requires(self):
        if self.ptask!='auto':
            self.pipelines = (self.ptask,)

        logger.info('Luigi is running the Semanic Pipeline on the date: {0}'.\
                    format( self.current_date))
        return [UpdatePlotsDB(semantic_task=semantic_task,
                              current_date=self.current_date,
                              plot_oriented=configuration.get_config().\
                                     get(semantic_task, 'plot_oriented'))
                for semantic_task in self.pipelines]

class UpdatePlotsDB(luigi.Task):

    semantic_task = luigi.Parameter()
    current_date = luigi.DateParameter()
    historical = luigi.Parameter('DEFAULT')
    plot_oriented = luigi.Parameter()

    # AWS RDS
    database = os.environ.get("PGDATABASE")
    user = os.environ.get("POSTGRES_USER")
    password = os.environ.get("POSTGRES_PASSWORD")
    host = os.environ.get("PGHOST")
    port = os.environ.get("PGPORT")

    engine = create_engine('postgresql://{0}:{1}@{2}:{3}/{4}'.\
                            format(user, password, host,
                                   port, database))

    @property
    def update_id(self):
        return str(self.plots_task) + '_plots'

    @property
    def table(self):
        return "plots." + self.semantic_task

    @property
    def plots_data:
        try:
            plots_data = pd.read_sql("SELECT * FROM plots.{}".\
                                     format(self.semantic_task), con=self.engine)
            return plots_data
        except:
            return None

    @property
    def updates:
        updates_data = pd.DataFrame(columns=['schema', 'table_name',\
                                                        'last_update'])
        if(self.plots_data):
            unique_tables = pd.DataFrame([(row.get('schema'),\
                                            row.get('table_name'))\
                                                  for row in plots_data.metadata],
                                          columns=['schema', 'table_name']).\
                            drop_duplicates()
            # Test if we need first or last element
            for index, row in unique_tables.iterrows():
                try:
                    if row['schema'] == 'features':
                        data_dates = get_feature_dates(row['table_name'],
                                                        self.current_date)
                    else:
                        data_dates = get_final_dates(row['table_name'],
                                                        self.current_date)
                    last_update = data_dates[0][-1]
                except:
                    data_dates = ([''], '')
                updates_data.append([schema, table, last_update])
        return updates_data

    def run:
        if (self.plot_oriented):
            updated_plots_data = plots_test(updates_data=self.updates_data,
                                                plots_data=self.plots_data)
            if updated_plots_data != self.plots_data:
                query = """DROP TABLE IF EXISTS plots.{0};""".\
                        format(self.semantic_task)
                engine.execute(query)
                tested_data.to_sql(name=self.semantic_task,
                                   con=engine,
                                   schema='plots')

    def requires:
        return(UpdateSemanticDB(semantic_task=self.semantic_task,
                                current_date=self.current_date)

    def output:
        return PostgresTarget(host=self.host,
                              database=self.database,
                              user=self.user,
                              password=self.password,
                              table=self.table,
                              update_id=self.update_id)

class UpdateSemanticDB(postgres.PostgresQuery):

    semantic_task = luigi.Parameter()
    current_date = luigi.DateParameter()
    client = S3Client()
    semantic_scripts = luigi.Parameter()
    historical = luigi.Parameter('DEFAULT')

    # RDS
    database = os.environ.get("PGDATABASE")
    user = os.environ.get("POSTGRES_USER")
    password = os.environ.get("POSTGRES_PASSWORD")
    host = os.environ.get("PGHOST")

    @property
    def update_id(self):
        return str(self.semantic_task) + '_semantic'

    @property
    def table(self):
        return "semantic." + self.semantic_task

    @property
    def query(self):
        # Read sql command
        path = self.semantic_scripts + self.semantic_task + '.sql'

        try:
            sqlfile = open(path, 'r')
            query = sqlfile.read()

        except:
            query = ("""DROP TABLE IF EXISTS {0};
                     CREATE TABLE {0} AS (SELECT * FROM tidy.{1});"""
                     .format(self.table, self.semantic_task))
        return query

    def requires(self):
        return UpdateTidyDB(current_date=self.current_date,
                            semantic_task=self.semantic_task)

    def output(self):
        return PostgresTarget(host=self.host,
                              database=self.database,
                              user=self.user,
                              password=self.password,
                              table=self.table,
                              update_id=self.update_id)

class UpdateTidyDB(PgRTask):

    """
    This Task runs the tidy script in tidy folder for
    the features task, if it doesn't exist then it runs
    the no_tidy.R script from the same folder.
    """

    current_date = luigi.DateParameter()
    semantic_task = luigi.Parameter()
    client = S3Client()
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
                        '--database', self.database,
                        '--user', self.user,
                        '--password', "'{}'".format(self.password),
                        '--host', self.host,
                        '--semantic', self.semantic_task]
        cmd = " ".join(command_list)
        return cmd

    @property
    def update_id(self):
        return str(self.semantic_task) + '_tidy'

    @property
    def table(self):
        return "tidy." + self.pipeline_task

    def requires(self):
        # Check table dependencies
        dep_types = [dt for dt in composition[self.semantic_task].keys()]

        for dt in dep_types:
            if 'features_dependencies' in dep_types:
                features_tables = composition[self.semantic_task]['features_dependencies']
                yield [FeaturesPipeline(current_date=self.current_date,
                                        ptask=pipeline_task) for pipeline_task\
                                                in features_tables]

            if 'clean_dependencies' in dep_types:
                clean_tables = composition[self.semantic_task]['clean_dependencies']
                yield [ETLPipeline(current_date=self.current_date,
                                   ptask=pipeline_task) for pipeline_task\
                                           in clean_tables]

            if 'model_dependencies' in dep_types:
                models_tables = composition[self.semantic_task]['models_dependencies']
                yield [ModelsPipeline(current_date=self.current_date,
                                      ptask=pipeline_task) for pipeline_task\
                                              in models_tables]

    def output(self):
        return PostgresTarget(host=self.host,
                              database=self.database,
                              user=self.user,
                              password=self.password,
                              table=self.table,
                              update_id=self.update_id)
