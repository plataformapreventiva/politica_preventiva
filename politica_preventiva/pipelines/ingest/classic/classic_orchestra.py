#!/usr/bin/env python
# -*- coding: utf-8 -*-
import luigi
import logging
import os
import yaml
import ast
import tempfile
import boto3
import re
import psycopg2
import numpy as np

from dotenv import find_dotenv
from itertools import product
from luigi import six
from luigi import configuration
from luigi.contrib import postgres
from luigi.contrib.s3 import S3Target, S3Client
from os.path import join
from pathlib import Path
from py2neo import Graph, Node, Relationship


from politica_preventiva.pipelines.ingest.classic.classic_ingest_tasks import *
from politica_preventiva.pipelines.ingest.classic.emr_ingest_tasks import *
from politica_preventiva.pipelines.ingest.classic.\
    preprocessing_scripts.preprocessing_scripts import *
from politica_preventiva.pipelines.ingest.tools.ingest_utils import parse_cfg_list, \
    extras, dates_list, get_extra_str, s3_to_pandas, final_dates
from politica_preventiva.pipelines.utils import s3_utils
from politica_preventiva.pipelines.ingest.tests import classic_tests
from politica_preventiva.tasks.pipeline_task import *
from politica_preventiva.pipelines.utils import emr_tasks
from politica_preventiva.pipelines.ingest.geoms.geoms_orchestra import *

# Load Postgres Schemas
with open('./pipelines/ingest/common/raw_schemas.yaml', 'r') as file:
    header_d = yaml.load(file)
open('./pipelines/ingest/common/raw_schemas.yaml').close()
conf = configuration.get_config()

# AWS
aws_access_key_id = os.environ.get('AWS_ACCESS_KEY_ID')
aws_secret_access_key = os.environ.get('AWS_SECRET_ACCESS_KEY')
PLACES_API_KEY = os.environ.get('PLACES_API_KEY')

# EMR
with open("pipelines/ingest/common/emr-config.yaml", "r") as file:
        config = yaml.load(file)
        config_emr = config.get("emr")

# logger
logging_conf = configuration.get_config().get("core", "logging_conf_file")
logging.config.fileConfig(logging_conf)
logger = logging.getLogger("dpa-sedesol")

#######################
# Classic Ingest Tasks
#########


class UpdateRawDB(postgres.CopyToTable):

    """ Updates Postgres DataBase with new ingested data.
        This Task compares and updates the new data from the
        concatenation folder in the s3 bucket with the previous
        loaded data in the postgres DB.
        Note
        ---------
        Assumes that the schema of the pipeline_task is defined
        (see. commons/pg_raw_schemas)
        Returns
        ---------
        Returns a PostgresTarget representing the inserted dataset
    """

    current_date = luigi.DateParameter()
    actualizacion = luigi.DateParameter()
    pipeline_task = luigi.Parameter()
    data_date = luigi.Parameter()
    suffix = luigi.Parameter()
    # extra = luigi.Parameter()

    client = S3Client()
    local_path = luigi.Parameter('DEFAULT')  # path where csv is located
    raw_bucket = luigi.Parameter('DEFAULT')  # s3 bucket address

    # RDS
    database = os.environ.get("PGDATABASE")
    user = os.environ.get("POSTGRES_USER")
    password = os.environ.get("POSTGRES_PASSWORD")
    host = os.environ.get("PGHOST")

    def requires(self):
        return Concatenation(current_date=self.current_date,
                             pipeline_task=self.pipeline_task,
                             data_date=self.data_date, suffix=self.suffix)

    @property
    def columns(self):
       logging.info("Trying to update the DB for"+\
            " the pipeline_task {0} ".format(self.pipeline_task +\
            "data_date-{0}-suffix-{1}".format(self.data_date, self.suffix)))

       try:
            temp = header_d[self.pipeline_task]['LUIGI']['SCHEMA']
            temp = str(temp).replace(
                "{", "(").replace("}", ")").replace(":", ",")
            return ast.literal_eval(temp)
       except:
           logger.exception("Please define the schema and variable types"+
                         "for this table  pipeline_task  {0} -"+
                         "data_date-{1}-suffix-{2}".format(
                self.pipeline_task, self.data_date, self.suffix))

    @property
    def table(self):
        return "raw." + self.pipeline_task

    @property
    def update_id(self):
        return str(self.pipeline_task) + str(self.data_date) +\
                str(self.suffix) + 'db'

    def rows(self):
        """
        Returs lists corresponding to each row to be inserted.
        """

        s3 = boto3.client('s3')
        keyname = self.input().path.split('preventiva/')[1]
        response = s3.head_object(Bucket='dpa-plataforma-preventiva', Key=keyname)
        # If the file is greater than 5 gb, ingest line per line
        # TODO append B as a list to respect the data type
        if response['ContentLength'] > 10000: #5368709120:
            with self.input().open('r') as fobj:
                check = True
                for line in fobj:
                    if check:
                        check = False
                        header = line
                        pass
                    # Skip header line
                    elif line == header:
                        pass
                    else:
                        line = re.sub("^na\||\|N/A\||\|na$\||\|na\||\|nan\||\|N/E\||\|-\||\|\s+\|", '||', line)
                        line = re.sub("^na\||\|N/A\||\|na$\||\|na\||\|nan\||\|N/E\||\|-\||\|\s+\|", '||', line)
                        line = re.sub("\|\s+\|", '||', line)

                        if re.findall("^[\||\s]+$", line) != []:
                            pass
                        else:
                            line = line.strip('\n').split('|')
                            line.append(self.actualizacion.strftime("%Y-%m-%d %H:%M:%S"))
                            line.append(str(self.data_date) + '-' + self.suffix)
                            yield [x if x != '' else None for x in line]

        else:
            # TODO() remove this step, checkout for
            # concatenation and header removal.
            data = pd.read_csv(self.input().path, sep="|", encoding="utf-8",
                               dtype=str, error_bad_lines=False, header=None)
            data.drop_duplicates(keep='first', inplace=True)
            data = data.replace('nan|N/E|^\-$', np.nan, regex=True)
            data = data.where((pd.notnull(data)), None)
            data = data.iloc[1:]
            data[len(data.columns)] = self.actualizacion
            data[len(data.columns)] = str(self.data_date) + "-" + self.suffix
            return [tuple(x) for x in data.to_records(index=False)]

    def copy(self, cursor, file):
        connection = self.output().connect()

        if isinstance(self.columns[0], six.string_types):
            column_names = self.columns

        elif len(self.columns[0]) == 2:
            # Create columns for csv upload
            column_names = [c[0] for c in self.columns]
            create_sql = [join(c[0], ' ', c[1]) for c in self.columns]
            # Create string for table creation
            create_sql = ', '.join(create_sql).replace("/", "")
            # Create string for Upsert
            unique_sql = ', '.join(column_names)
        else:
            logger.exception('columns must consist of column strings\
            or (column string, type string)\
            tuples (was %r ...)' % (self.columns[0],))
            sys.exit()

        index = header_d[self.pipeline_task]['LUIGI']["INDEX"]
        # Create temporary table temp
        cmd = "CREATE TEMPORARY TABLE tmp  ({0}); ANALYZE tmp; ".\
            format(create_sql)
        cmd += 'CREATE INDEX IF NOT EXISTS {0}_index ON\
         tmp ({0});'.format(index, self.pipeline_task)
        cursor.execute(cmd)
        # Copy to TEMP table
        cursor.copy_from(file, "tmp", null=r'\\N', sep=self.column_separator,
                         columns=column_names)

        # Check if raw table exists if not create and build index
        # (defined in common/pg_raw_schemas)
        cmd = "CREATE TABLE IF NOT EXISTS {0} ({1}); ".format(self.table, create_sql)
        cmd += 'CREATE INDEX IF NOT EXISTS {0}_index ON {1} ({0});'.format(index, self.table)

        # SET except from TEMP to raw table ordered
        cmd += "INSERT INTO {0} \
            (SELECT {1} FROM tmp);".\
            format(self.table, unique_sql)
            # EXCEPT SELECT {1} FROM {0} ;".\
        cursor.execute(cmd)
        connection.commit()

        return True

    def run(self):

        if not (self.table and self.columns):
            raise Exception("table and columns need to be specified")

        connection = self.output().connect()
        tmp_dir = luigi.configuration.get_config().get('postgres',
                                                       'local-tmp-dir', None)
        tmp_file = tempfile.TemporaryFile(dir=tmp_dir)
        n = 0
        for row in self.rows():
            n += 1
            rowstr = self.column_separator.join(
                self.map_column(val) for val in row)
            rowstr += "\n"
            tmp_file.write(rowstr.encode('utf-8'))
        tmp_file.seek(0)

        for attempt in range(2):

            try:
                cursor = connection.cursor()
                self.init_copy(connection)
                self.copy(cursor, tmp_file)
                self.post_copy(connection)

            except psycopg2.ProgrammingError as e:
                if e.pgcode == psycopg2.errorcodes.UNDEFINED_TABLE and\
                        attempt == 0:

                    # if first attempt fails with "relation not found", try
                    # creating table
                    connection.reset()
                    self.create_table(connection)
                else:
                    raise
            else:
                break

        connection.commit()

        # mark as complete using our update_id defined above
        self.output().touch(connection)

        # commit and clean up
        connection.commit()
        connection.close()
        tmp_file.close()

        # Remove last processing file
        self.client.remove(self.raw_bucket + self.pipeline_task +
                           "/concatenation/" + self.data_date + "/")

    def output(self):
        return postgres.PostgresTarget(host=self.host,
                                       database=self.database,
                                       user=self.user,
                                       password=self.password,
                                       table=self.table,
                                       update_id=self.update_id)


class Concatenation(luigi.Task):

    """ Concatenation Task.

        Note
        ---------

        Returns
        ---------
    """

    current_date = luigi.DateParameter()
    pipeline_task = luigi.Parameter()
    client = S3Client()
    data_date = luigi.Parameter()
    suffix = luigi.Parameter()

    raw_bucket = luigi.Parameter('DEFAULT')

    def requires(self):
        extra = extras(self.pipeline_task)
        if extra[0] == 'spark':

            emr_task = eval(self.pipeline_task + 'EMR')
            return emr_task(pipeline_task=self.pipeline_task,
                            current_date=self.current_date,
                            data_date=self.data_date,
                            suffix=self.suffix,
                            id_name=self.pipeline_task + '_' + self.data_date)

        else:
            returns = []
            for extra_p in extra:
                returns.append(Preprocess(pipeline_task=self.pipeline_task,
                                  current_date=self.current_date,
                                  extra=extra_p, data_date=self.data_date,
                                  suffix=self.suffix))
            return returns

    def run(self):

        result_filepath = self.pipeline_task + "/concatenation/" + \
                           self.data_date + "/" + self.pipeline_task + '.csv'
        # folder to concatenate
        folder_to_concatenate = self.pipeline_task + "/preprocess/" +\
                self.data_date + "/"
        # function for appending all .csv files in folder_to_concatenate
        s3_utils.run_concatenation(self.raw_bucket, folder_to_concatenate,
                                   result_filepath, '.csv')

        # Delete files in preprocess
        self.client.remove(self.raw_bucket + folder_to_concatenate)

    def output(self):
        return S3Target(path=self.raw_bucket + self.pipeline_task +
                        "/concatenation/" + self.data_date + "/" +
                        self.pipeline_task + '.csv')


class Preprocess(luigi.Task):

    """ Concatenation Task.
        Note
        ---------
        Returns
        ---------
    """

    current_date = luigi.DateParameter()
    pipeline_task = luigi.Parameter()
    data_date = luigi.Parameter()
    extra = luigi.Parameter()
    client = S3Client()
    suffix = luigi.Parameter()

    raw_bucket = luigi.Parameter('DEFAULT')

    def requires(self):

        return LocalToS3(data_date=self.data_date,
                         pipeline_task=self.pipeline_task,
                         extra=self.extra,
                         suffix=self.suffix)

    def run(self):

        extra_h = get_extra_str(self.extra)

        key = self.pipeline_task + "/raw/" + self.data_date +\
            "-" + self.suffix + "-" + \
            self.pipeline_task + extra_h + ".csv"
        out_key = "etl/" + self.pipeline_task + "/preprocess/" +\
                self.data_date + "/" + self.data_date + "--" +\
                self.pipeline_task + extra_h + ".csv"
        try:
            preprocess_tasks = eval(self.pipeline_task + '_prep')
            preprocess_tasks(data_date=self.data_date, s3_file=key,
                             extra_h=extra_h, out_key=out_key)
        except:
            no_preprocess_method(data_date=self.data_date,
                                 s3_file=key, extra_h=extra_h,
                                 out_key=out_key)

    def output(self):
        extra_h = get_extra_str(self.extra)
        return S3Target(path=self.raw_bucket + self.pipeline_task +
                        "/preprocess/" + self.data_date + "/" +
                        self.data_date + "--" +
                        self.pipeline_task + extra_h + ".csv")


class LocalToS3(luigi.Task):

    """ LocalToS3 Task.
        Note
        ---------

        Returns
        ---------
    """

    pipeline_task = luigi.Parameter()
    extra = luigi.Parameter()
    client = S3Client(aws_access_key_id, aws_secret_access_key)
    data_date = luigi.Parameter()
    suffix = luigi.Parameter()

    local_path = luigi.Parameter('DEFAULT')  # path where csv is located
    docker_path = luigi.Parameter('DEFAULT')
    raw_bucket = luigi.Parameter('DEFAULT')  # s3 bucket address

    def requires(self):
        extra_h = get_extra_str(self.extra)
        local_ingest_file = self.local_path + self.pipeline_task +\
            "/" + self.data_date + "-" + self.suffix + "-" + self.pipeline_task +\
            extra_h + ".csv"

        task = RawHeaderTest(pipeline_task=self.pipeline_task,
                             data_date=self.data_date,
                             local_ingest_file=local_ingest_file,
                             extra=self.extra,
                             suffix=self.suffix)
        return task

    def run(self):

        extra_h = get_extra_str(self.extra)
        local_ingest_file = self.local_path + self.pipeline_task +\
            "/" + self.data_date + "-" + self.suffix + "-" + self.pipeline_task +\
            extra_h + ".csv"

        try:
            self.client.put(local_ingest_file,
                            self.output().path)
        except:
            logger.debug('The raw file of the pipeline {0}'.format(self.pipeline_task) +\
            ' is kinda big, Luigi will try to upload it in chunks')
            output = self.output().path.split('preventiva/')[1]
            s3_utils.big_file_to_s3(local_ingest_file, output)

        os.remove(local_ingest_file + ".done")

    def output(self):
        extra_h = get_extra_str(self.extra)

        return S3Target(path=self.raw_bucket + self.pipeline_task + "/raw/" +
                        self.data_date + "-" + self.suffix + "-" +
                        self.pipeline_task + extra_h + ".csv")


class RawHeaderTest(luigi.Task):

    """ RawHeaderTest Task.
        Checks if the structure of the raw source data has changed

        Note
        ---------
        Assumes that the source header schema is defined in:
        common/raw_schemas.yaml

    """
    pipeline_task = luigi.Parameter()
    data_date = luigi.Parameter()
    extra = luigi.Parameter()
    local_ingest_file = luigi.Parameter()
    suffix = luigi.Parameter()

    classic_task_scripts = luigi.Parameter('DEFAULT')
    common_path = luigi.Parameter('DEFAULT')
    new = luigi.Parameter('DEFAULT')

    def requires(self):

        return LocalIngest(pipeline_task=self.pipeline_task,
                           data_date=self.data_date,
                           local_ingest_file=self.local_ingest_file,
                           extra=self.extra)

    def run(self):
        classic_tests.header_test(self.input().path,
                                  self.pipeline_task,
                                  self.common_path,
                                  self.suffix,
                                  self.new)

    def output(self):
        done = self.local_ingest_file + ".done"
        return luigi.LocalTarget(done)


class LocalIngest(luigi.Task):

    """ LocalIngest Task.
        Handler for pipeline_task crawlers

        Note
        ---------
        Assumes that the specific pipeline_task crawler is in folder:
        ingest/classic/source/

    """

    pipeline_task = luigi.Parameter()
    data_date = luigi.Parameter()
    local_ingest_file = luigi.Parameter()
    extra = luigi.Parameter()

    def requires(self):
        try:
            classic_tasks = eval(self.pipeline_task)
        except:
            classic_tasks =  general_ingest
            # logger.info('General Ingest')

        task = classic_tasks(data_date=self.data_date,
                             pipeline_task=self.pipeline_task,
                             local_ingest_file=self.local_ingest_file,
                             extra=self.extra)
        return task

    def output(self):
        return luigi.LocalTarget(self.local_ingest_file)
