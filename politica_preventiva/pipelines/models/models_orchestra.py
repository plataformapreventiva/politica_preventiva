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

from politica_preventiva.pipelines.utils.pg_tools import PGWrangler
from politica_preventiva.tasks.pipeline_task import DockerTask, ModelTask
from politica_preventiva.pipelines.ingest.ingest_orchestra import UpdateLineage
from politica_preventiva.pipelines.ingest.tools.ingest_utils import parse_cfg_list,\
    extras, dates_list, get_extra_str, s3_to_pandas, final_dates
from politica_preventiva.pipelines.utils import s3_utils
from politica_preventiva.pipelines.etl.etl_orchestra import UpdateCleanDB
from politica_preventiva.pipelines.ingest.tools.ingest_utils import final_dates


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
    """
    current_date = luigi.DateParameter()
    models = parse_cfg_list(conf.get("ModelsPipeline", "pipelines"))
    client = S3Client()

    def requires(self):
        set_pipelines = [(model_task, final_dates(False,
                                                 model_task,
                                                 self.current_date)) for
                        model_task in self.models]
        return [RunModel(model_task, self.current_date)
                for model_task in self.models]


class RunModel(ModelTask):
    """
    This Task runs the tidy script in tidy folder for
    the model_task, if it doesn't exists then it runs
    the no_tidy.R script from the same folder.
    """

    model_task = luigi.Parameter()
    current_date = luigi.DateParameter()
    client = S3Client()

    # RDS
    database = os.environ.get("PGDATABASE")
    user = os.environ.get("POSTGRES_USER")
    password = os.environ.get("POSTGRES_PASSWORD")
    host = os.environ.get("PGHOST")

    @property
    def cmd(self):
        # path and key to clone model repository
        env_variables = '-e token=' + os.environ.get("GIT_TOKEN") +\
                        '-e path=' + self.model_task
        # Path of the script
        model_script = self.model_task + '/models/' +\
            self.model_task + '.R'
        # TODO() Add model language types
        # Correr un modelo que puede ser R o python
        # Por ahora que solo sea R

        command_list = [env_variables,
                        'Rscript', model_script,
                        '--data_date', self.data_date,
                        '--database', self.database,
                        '--user', self.user,
                        '--password', "'{}'".format(self.password),
                        '--host', self.host,
                        '--pipeline', self.model_task]

        return " ".join(command_list)

    @property
    def update_id(self):
        return str(self.model_task) + '_' #+ str(self.data_date) +\
               # str(self.suffix) + '_models'

    @property
    def table(self):
        return "models." + self.model_task

    def requires(self):
        return ModelDependencies(current_date=self.current_date,
                                 model_task=self.model_task)#,
                                 #data_date=self.data_date,
                                 #suffix=self.suffix)

    def output(self):
        return PostgresTarget(host=self.host,
                              database=self.database,
                              user=self.user,
                              password=self.password,
                              table=self.table,
                              update_id=self.update_id)


class ModelDependencies(luigi.Task):
    """
    """

    model_task = luigi.Parameter()
    current_date = luigi.DateParameter()
    client = S3Client()

    # RDS
    database = os.environ.get("PGDATABASE")
    user = os.environ.get("POSTGRES_USER")
    password = os.environ.get("POSTGRES_PASSWORD")
    host = os.environ.get("PGHOST")

    def requires(self):

        # Check table dependencies
        model_tables = set([*composition]) & \
                set(composition[self.model_task]['dependencies'])
        dep_tables = set(composition[self.model_task]['dependencies']) - \
                set([*composition])

        yield [UpdateCleanDB(current_date=self.current_date,
                             pipeline_task=pipeline_task,
                             data_date=self.data_date)
               for pipeline_task in dep_tables]

        yield [RunModel(current_Date=self.current_date,
                        model_task=model_task,
                        data_date=self.data_date)
               for model_task in model_tables]
