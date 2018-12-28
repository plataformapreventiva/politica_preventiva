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
from politica_preventiva.pipelines.features.tools.pipeline_tools import get_features_dates
from politica_preventiva.pipelines.models.models_orchestra import\
        ModelsPipeline
from politica_preventiva.pipelines.features.features_orchestra import\
        FeaturesPipeline
from politica_preventiva.pipelines.etl.etl_orchestra import ETLPipeline

configuration.LuigiConfigParser.add_config_path('./pipelines/configs/luigi_semantic.cfg')

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
    semantic_composition = yaml.load(file)
with open("pipelines/configs/tidy_dependencies.yaml", "r") as file:
    composition = yaml.load(file)

class SemanticPipeline(luigi.WrapperTask):
    """
    """

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

        return [UpdateSemanticDB(semantic_task=semantic_task,
                              current_date=self.current_date)
               for semantic_task in self.pipelines]

class UpdateSemanticDB(PgRTask):
    """
    This task creates the semantic tables joining
    tidy tables defined in
    """

    semantic_task = luigi.Parameter()
    current_date = luigi.DateParameter()
    client = S3Client()
    semantic_scripts = luigi.Parameter()

    # RDS
    database = os.environ.get("PGDATABASE")
    user = os.environ.get("POSTGRES_USER")
    password = os.environ.get("POSTGRES_PASSWORD")
    host = os.environ.get("PGHOST")

    prod_database = os.environ.get("PROD_DATABASE")
    prod_user = os.environ.get("PROD_POSTGRES_USER")
    prod_password = os.environ.get("PROD_POSTGRES_PASSWORD")
    prod_host = os.environ.get("PROD_PGHOST")


    @property
    def update_id(self):
        return str(self.semantic_task) + '_semantic_'

    @property
    def table(self):
        return "semantic." + self.semantic_task

    @property
    def cmd(self):
        semantic_file = self.semantic_scripts +\
                    self.semantic_task + '.R'
        extra_parameters = ",".join(semantic_composition[self.semantic_task]['tidy_dependencies'])

        command_list = ['Rscript', semantic_file,
                        '--database', self.database,
                        '--user', self.user,
                        '--password', "'{}'".format(self.password),
                        '--host', self.host,
                        '--prod_database', self.prod_database,
                        '--prod_user', self.prod_user,
                        '--prod_password', "'{}'".format(self.prod_password),
                        '--prod_host', self.prod_host,
                        '--pipeline', self.semantic_task,
                        '--extra_parameters', extra_parameters]
        cmd = " ".join(command_list)
        return cmd

    def requires(self):
        dep = semantic_composition[self.semantic_task]
        if dep!=None:
            dep_types = [dt for dt in dep.keys()]
        else:
            return
        for dt in dep_types:
            if 'tidy_dependencies' in dep_types:
                tidy_tables = semantic_composition[self.semantic_task]['tidy_dependencies']
                yield [MetadataTidyDB(current_date=self.current_date,
                                        tidy_task=pipeline_task) for pipeline_task\
                                                in tidy_tables]

    def output(self):
        return PostgresTarget(host=self.host,
                              database=self.database,
                              user=self.user,
                              password=self.password,
                              table=self.table,
                              update_id=self.update_id)

class MetadataTidyDB(PgRTask):

    """
    This Task runs the tidy script in tidy folder for
    the features task, if it doesn't exist then it runs
    the no_tidy.R script from the same folder.
    """

    current_date = luigi.DateParameter()
    tidy_task = luigi.Parameter()
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
                    'metadata.R'

        if os.path.isfile(tidy_file):
            pass
        else:
            raise

        command_list = ['Rscript', tidy_file,
                        '--database', self.database,
                        '--user', self.user,
                        '--password', "'{}'".format(self.password),
                        '--host', self.host,
                        '--pipeline', self.tidy_task]
        cmd = " ".join(command_list)
        return cmd

    @property
    def update_id(self):
        return str(self.tidy_task) + 'metadata_tidy'

    @property
    def table(self):
        return "tidy." + self.tidy_task + '_metadata'

    def requires(self):
        return UpdateTidyDB(current_date=self.current_date,
                tidy_task=self.tidy_task)

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
    tidy_task = luigi.Parameter()
    client = S3Client()
    tidy_scripts = luigi.Parameter()

    # RDS
    database = os.environ.get("PGDATABASE")
    user = os.environ.get("POSTGRES_USER")
    password = os.environ.get("POSTGRES_PASSWORD")
    host = os.environ.get("PGHOST")

    @property
    def cmd(self):
        plot_oriented = conf.get(self.tidy_task, "plot_oriented")
        extra_parameters = ','.join(parse_cfg_list(conf.get(self.tidy_task,
          "extra_parameters")))

        tidy_file = self.tidy_scripts +\
                    self.tidy_task + '.R'

        if os.path.isfile(tidy_file):
            pass
        else:
            tidy_file = self.tidy_scripts + 'no_tidy.R'

        command_list = ['Rscript', tidy_file,
                        '--database', self.database,
                        '--user', self.user,
                        '--password', "'{}'".format(self.password),
                        '--host', self.host,
                        '--pipeline', self.tidy_task,
                        '--extra_parameters', extra_parameters]
        cmd = " ".join(command_list)
        return cmd

    @property
    def update_id(self):
        return str(self.tidy_task) + '_tidy'

    @property
    def table(self):
        return "tidy." + self.tidy_task

    def requires(self):
        # Check table dependencies
        dep = composition[self.tidy_task]
        if dep!=None:
            dep_types = [dt for dt in dep.keys()]
        else:
            return

        for dt in dep_types:
            if 'features_dependencies' in dep_types:
                features_tables = composition[self.tidy_task]['features_dependencies']
                yield [FeaturesPipeline(current_date=self.current_date,
                                        ptask=pipeline_task) for pipeline_task\
                                                in features_tables]

            if 'clean_dependencies' in dep_types:
                clean_tables = composition[self.tidy_task]['clean_dependencies']
                yield [ETLPipeline(current_date=self.current_date,
                                   ptask=pipeline_task) for pipeline_task\
                                           in clean_tables]

            if 'model_dependencies' in dep_types:
                models_tables = composition[self.tidy_task]['models_dependencies']
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
