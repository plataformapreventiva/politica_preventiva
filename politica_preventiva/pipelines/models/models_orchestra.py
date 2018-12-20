#!/usr/bin/env python
# coding: utf-8

import datetime
import luigi
import os
import random
import re
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

from politica_preventiva.pipelines.utils.pg_tools import PGWrangler
from politica_preventiva.tasks.pipeline_task import DockerTask, ModelTask
from politica_preventiva.pipelines.ingest.ingest_orchestra import UpdateLineage
from politica_preventiva.pipelines.utils.pipeline_utils import parse_cfg_list,\
    extras, dates_list, get_extra_str, s3_to_pandas, final_dates
from politica_preventiva.pipelines.utils import s3_utils
from politica_preventiva.pipelines.etl.etl_orchestra import UpdateCleanDB
from politica_preventiva.pipelines.ingest.tools.ingest_utils import final_dates
from politica_preventiva.pipelines.features.features_orchestra import FeaturesPipeline
from politica_preventiva.pipelines.etl.etl_orchestra import ETLPipeline
from politica_preventiva.pipelines.utils.pipeline_utils import final_dates

# Env Setup
load_dotenv(find_dotenv())

# Logger & Config
conf = configuration.get_config()

logging_conf = configuration.get_config().get("core", "logging_conf_file")
logging.config.fileConfig(logging_conf)
logger = logging.getLogger("dpa-sedesol")

# AWS
aws_access_key_id = os.environ.get('AWS_ACCESS_KEY_ID')
aws_secret_access_key = os.environ.get('AWS_SECRET_ACCESS_KEY')


# Get Models Dependencies
with open("pipelines/configs/model_dependencies.yaml", 'r') as file:
    composition = yaml.load(file)


class ModelsPipeline(luigi.WrapperTask):

    """
    This task programs the model dependencies and defines the missing
    data dates for the model.
    """
    current_date = luigi.DateParameter()
    models = parse_cfg_list(conf.get("ModelsPipeline", "pipelines"))
    client = S3Client()
    ptask = luigi.Parameter()

    def requires(self):
        if self.ptask != "auto":
            self.models = [self.ptask]

        set_pipelines = [(model_task, final_dates(model_task,
                                                 self.current_date)) for
                        model_task in self.models]

        return [RunModel(model_task=model_task[0],
                         data_date=dates,
                         current_date=self.current_date)
                for model_task in set_pipelines
                for dates in model_task[1][0]]


class RunModel(ModelTask):

    """
    This Task creates the bash command to run the model inside the
    model-task docker image.   All the models task are currently
    defined for R scripts, which receive DB connection settings and
    a data_date as input parameters.
    """

    model_task = luigi.Parameter()
    current_date = luigi.DateParameter()
    data_date = luigi.Parameter()
    client = S3Client()

    # RDS
    database = os.environ.get("PGDATABASE")
    user = os.environ.get("POSTGRES_USER")
    password = os.environ.get("POSTGRES_PASSWORD")
    host = os.environ.get("PGHOST")

    @property
    def cmd(self):
        # path and key to clone model repository
        env_variables = " -e token=" + os.environ.get("GIT_TOKEN") +\
                        " -e path=" + composition[self.model_task]["repository"][0]

        # Path of the script named by the model tasks
        # you can have multiple models on the same repository
        model_script = "models/" + self.model_task + "/handler.R"
        password = self.password.replace('$','\$')

        # TODO() Add model language types
        command_list = [' -e cmd="Rscript', model_script,
                        "--data_date", self.data_date,
                        "--current_date", str(self.current_date),
                        "--database", self.database,
                        "--user", self.user,
                        "--password '{0}'".format(password),
                        "--host", self.host,
                        "--pipeline", self.model_task, '"']
        return env_variables + " ".join(command_list)

    @property
    def update_id(self):
        return str(self.model_task) + '_' + str(self.data_date)

    @property
    def table(self):
        return "models." + self.model_task

    def requires(self):
        # Check table dependencies
        dep_types = [*composition[self.model_task]]

        if 'features_dependencies' in dep_types:
            features_tables = composition[self.model_task]['features_dependencies']
            yield [FeaturesPipeline(current_date=self.current_date,
                pipelines=[pipeline_task],ptask='auto') for pipeline_task in features_tables]

        if 'clean_dependencies' in dep_types:
            clean_tables = composition[self.model_task]['clean_dependencies']
            yield [ETLPipeline(current_date=self.current_date,
                pipelines=[pipeline_task],ptask='auto') for pipeline_task in clean_tables]

        if 'model_dependencies' in dep_types:
            models_tables = composition[self.model_task]['models_dependencies']
            yield [ModelsPipeline(current_date=self.current_date,
                models=[pipeline_task],ptask='auto') for pipeline_task in models_tables]

    def output(self):
        return PostgresTarget(host=self.host,
                              database=self.database,
                              user=self.user,
                              password=self.password,
                              table=self.table,
                              update_id=self.update_id)

