#!/usr/bin/env python
# coding: utf-8
import ast
import datetime
import os
import pdb
import pandas as pd
import luigi
import logging
import yaml

from luigi import six, task
from luigi import configuration
from luigi.contrib import postgres
from luigi.s3 import S3Target, S3Client
from politica_preventiva.pipelines.ingest.classic.classic_orchestra import\
        UpdateRawDB
from dotenv import find_dotenv
from itertools import product
from os.path import join
from pathlib import Path
from py2neo import Graph, Node, Relationship

from politica_preventiva.pipelines.ingest.classic.classic_ingest_tasks import *
from politica_preventiva.pipelines.ingest.classic.\
    preprocessing_scripts.preprocessing_scripts import *
from politica_preventiva.pipelines.utils.pipeline_utils import\
        parse_cfg_list, extras, dates_list, get_extra_str, s3_to_pandas,\
        final_dates
from politica_preventiva.pipelines.utils import s3_utils
from politica_preventiva.pipelines.tests import pipeline_tests
from politica_preventiva.pipelines.ingest.tests import classic_tests
from politica_preventiva.tasks.pipeline_task import *
from politica_preventiva.pipelines.utils import emr_tasks

# Env Setup
load_dotenv(find_dotenv())

# Logger & Config
conf = configuration.get_config()

logging_conf = configuration.get_config().get("core", "logging_conf_file")
logging.config.fileConfig(logging_conf)
logger = logging.getLogger("dpa-sedesol")


# Load Postgres Schemas
with open('./pipelines/ingest/common/raw_schemas.yaml', 'r') as file:
    header_d = yaml.load(file)
conf = configuration.get_config()

# EMR
with open("pipelines/ingest/common/emr-config.yaml", "r") as file:
        config = yaml.load(file)
        config_emr = config.get("emr")


class IngestPipeline(luigi.WrapperTask):

    """
    This wrapper task executes ingest pipeline
    It expects a list specifying which ingest pipelines to run
        Define pipelines to run in luigi.cfg [ClassicIngest]
    """

    current_date = luigi.DateParameter()
    ptask = luigi.Parameter()
    pipelines = parse_cfg_list(conf.get("IngestPipeline", "pipelines"))

    def requires(self):
        if self.ptask!='auto':
            self.pipelines = [self.ptask]

        logger.info('Luigi is running the Ingest Pipeline on the date: {0}'.format(
                    self.current_date))
        logger.info('Running the following pipelines: {0}'.\
                    format(self.pipelines))

        return [IngestDates(current_date=self.current_date,
                            pipeline_task=pipeline)
                for pipeline in self.pipelines]


class IngestDates(luigi.WrapperTask):

    """
    Luigi task that defines de dates to run each pipeline_task.
        Note:
        -------
        Please define periodicity and start_date for each pipeline_task in
        luigi.cfg
    """

    current_date = luigi.DateParameter()
    pipeline_task = luigi.Parameter()
    client = S3Client()
    actualizacion = datetime.datetime.now()

    common_path = luigi.Parameter('DEFAULT')
    local_path = luigi.Parameter('DEFAULT')  # path where csv is located
    raw_bucket = luigi.Parameter('DEFAULT')  # s3 bucket address

    @property
    def dates(self):
        dates, self.suffix = final_dates(self.pipeline_task,
                                         self.current_date)

        return dates

    def requires(self):
        logger.info('For this pipeline_task {0} Luigi '.format(self.pipeline_task) +\
                    '\n will try to download the data of the\n following periods:{0}'.\
                    format(self.dates))
        return [UpdateLineage(current_date=self.current_date,
                              pipeline_task=self.pipeline_task,
                              data_date=str(data_date), suffix=self.suffix)
                for data_date in self.dates]


class UpdateLineage(luigi.Task):

    """ This Task updates the Neo4j lineage database.
        TODO()
        - Column nodes should be unique - Implement it in cypher!
        - Improve tags (tipo & subtipo) in the dictionary. Likely
            create a list rather than a single column. Define static tags.
        - Create Neo4j Task to collect output
        Note
        ---------
        Assumes that the dictionary of the pipeline_task is defined
        Returns TODO() Improve this element
        ---------
        Returns a temporal .done file saved in
            politica_preventiva/pipelines/ingest/common/neo4j
    """

    current_date = luigi.DateParameter()
    pipeline_task = luigi.Parameter()
    client = S3Client()
    actualizacion = datetime.datetime.now()
    data_date = luigi.Parameter()
    suffix = luigi.Parameter()

    common_path = luigi.Parameter('DEFAULT')
    local_path = luigi.Parameter('DEFAULT')  # path where csv is located
    raw_bucket = luigi.Parameter('DEFAULT')  # s3 bucket address

    def requires(self):
        return UpdateDictionary(current_date=self.current_date,
                                pipeline_task=self.pipeline_task,
                                data_date=self.data_date,
                                suffix=self.suffix)

    def run(self):

        logger.info('Creating ingestion Neo4j Lineage for the pipeline {0}'.\
            format(self.pipeline_task))

        # Reading from pipeline dictionary
        path = self.common_path + "dictionaries/"+self.pipeline_task +\
                "_dic.csv"
        nodes = pd.read_csv(path, sep='|', na_values='None',
                            keep_default_na=False)
        # actualizacion_sedesol = nodes.actualizacion_sedesol[0]
        nodes = nodes[nodes.id != 'actualizacion_sedesol']
        nodes = nodes[nodes.id != 'data_date']
        logging.info("Connecting to Neo4j DB {0} ")
        try:
            graph = Graph("http://localhost:7474/db/data/")
        except Exception as e:
            logging.exception("Failed to connect to database Neo4J" +\
                              " when attempting to connect ")
            raise('Failed to connect to database Neo4J')
        try:
            # Create Schema
            # ToDO() schema generation and constraints should be a task
            graph.schema.create_uniqueness_constraint('Table', 'value')
            graph.schema.create_uniqueness_constraint('Tag', 'tipo')
            graph.schema.create_uniqueness_constraint('Fraction', 'value')
            graph.schema.create_uniqueness_constraint('Column', 'nombre_clave')
            graph.schema.create_uniqueness_constraint('Year', 'year')
        except:
            pass
        # Define Nodes
        table = Node('Table', value=self.pipeline_task)
        years = self.data_date.split('-')[0]
        graph.merge(table)

        try:
            fraction = self.data_date.split('-')[1]
        except:
            fraction = ''

        try:
            # TODO() Clean this try/except or create luigi.neo4j.task
            Year = Node('Year', year=years)
            Fraction = Node('Fraction', value=str(years+'-'+fraction+'-'+self.suffix))
            relation = Relationship(Fraction, 'for_fraction', Year)
            graph.merge(Fraction)
            graph.merge(relation)

            for index, row in nodes.iterrows():
                try:
                    # Create column relations
                    column = Node('Column', nombre_clave=row['id'], nombre=row['nombre'])
                    graph.merge(column)
                    from_table = Relationship(column, 'from_table', table,
                                            pipeline=self.pipeline_task)
                    graph.merge(from_table)

                    if row['tipo'] != '':
                        try:
                            tag = Node('Tag', tipo=row['tipo'], subtipo=row['subtipo'])
                            relation = Relationship(column, 'with_tag', tag)
                            graph.merge(relation)
                        except:
                            #pdb.set_trace()
                            logger.info('Node with Tag: {tag} already exists.'.format(row['tipo']))
                except:
                    pass
                    # logger.debug('Your variabel ´ç'.format(row['tipo']))
            # Save current date
            current_date = row['actualizacion_sedesol']

            # Create Tag relations
            relation = Relationship(table, 'for_year', Fraction,
                                    updated=current_date)
            graph.merge(relation)

            done = self.common_path + 'neo4j/' + self.pipeline_task+\
                       self.data_date + '-'+self.suffix + '.done'
            Path(done).touch()

        except:
            pass

    def output(self):
        done = self.common_path + 'neo4j/' + self.pipeline_task+\
                self.data_date +'-'+self.suffix + '.done'

        return luigi.LocalTarget(done)


class UpdateDictionary(postgres.CopyToTable):

    """ Updates Postgres DataBase DIC with new Datadate ingested data.
        TODO(Update Neo4j table with the legacy nodes)
        Note
        ---------
        Assumes that the dictionary of the pipeline_task is defined
        Returns
        ---------
        Returns a PostgresTarget representing the inserted dataset
    """

    current_date = luigi.DateParameter()
    pipeline_task = luigi.Parameter()
    client = S3Client()
    actualizacion = datetime.datetime.now()
    data_date = luigi.Parameter()
    suffix = luigi.Parameter()

    common_path = luigi.Parameter('DEFAULT')
    local_path = luigi.Parameter('DEFAULT')  # path where csv is located
    raw_bucket = luigi.Parameter('DEFAULT')  # s3 bucket address

    common_bucket = luigi.Parameter()
    common_key = luigi.Parameter()

    # RDS
    database = os.environ.get("PGDATABASE")
    user = os.environ.get("POSTGRES_USER")
    password = os.environ.get("POSTGRES_PASSWORD")
    host = os.environ.get("PGHOST")

    @property
    def update_id(self):
        return str(self.pipeline_task) + str(self.data_date) +\
                str(self.suffix) + 'dic'

    @property
    def table(self):
        return "raw." + self.pipeline_task + "_dic"

    @property
    def columns(self):
        try:
            temp = header_d["dictionary"]['LUIGI']['SCHEMA']
            temp = str(temp).replace(
                "{", "(").replace("}", ")").replace(":", ",")
            return ast.literal_eval(temp)
        except Exception as e:
            logging.exception("The Dictionary schema abstraction is undefined"+\
                              " please define it in. common/raw_schemas.yaml")

    def rows(self):
        logging.info("Trying to update the dictionary for"+\
                     " the pipeline_task {0} ".format(self.pipeline_task +\
                     "data_date {0}".format(self.data_date)))

        header = [[a for (a, b) in
                   header_d["dictionary"]['LUIGI']['SCHEMA'][i].items()][0] for
                  i in range(len(self.columns))]
        table_header = [x for x in set().union(*(d.keys() for d \
                in header_d[self.pipeline_task]['LUIGI']['SCHEMA']))]
        path = self.common_path + "dictionaries/" +\
               self.pipeline_task + "_dic.csv"
        data = pipeline_tests.dictionary_test(task=self.pipeline_task,
                                              dict_path=path,
                                              table_header=table_header,
                                              dict_header=header,
                                              current_date=self.actualizacion,
                                              data_date=self.data_date,
                                              suffix=self.suffix,
                                              common_bucket=self.common_bucket,
                                              common_key=self.common_key)
        return [tuple(x) for x in data.to_records(index=False)]

    def requires(self):
        extra = extras(self.pipeline_task)
        return UpdateRawDB(current_date=self.current_date,
                           pipeline_task=self.pipeline_task,
                           actualizacion=self.actualizacion,
                           data_date=self.data_date, suffix=self.suffix)


    def output(self):
        return postgres.PostgresTarget(host=self.host, database=self.database,
                                       user=self.user, password=self.password,
                                       table=self.table,
                                       update_id=self.update_id)
