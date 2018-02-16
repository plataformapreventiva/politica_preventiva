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
from politica_preventiva.tasks.pipeline_task import DockerTask
from politica_preventiva.pipelines.ingest.tools.ingest_utils import parse_cfg_list,\
    extras, dates_list, get_extra_str, s3_to_pandas, final_dates
from politica_preventiva.pipelines.utils import s3_utils
from politica_preventiva.pipelines.etl.etl_orchestra import ETLPipeline

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

    def requires(self):
        return [UpdateSemanticDB(semantic_task, self.current_date)
                for semantic_task in self.semantics]

class UpdateSemanticDB(postgres.PostgresQuery):


    semantic_task = luigi.Parameter()
    current_date = luigi.DateParameter()
    client = S3Client()
    semantic_scripts = luigi.Parameter()

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
        return ETLPipeline(pipelines=composition[self.semantic_task],
                          current_date=self.current_date)

    def output(self):
        return PostgresTarget(host=self.host,
                              database=self.database,
                              user=self.user,
                              password=self.password,
                              table=self.table,
                              update_id=self.update_id)

