# -*- coding: utf-8 -*-
# import pdb
import os
import ast
import luigi
import psycopg2
import tempfile
import numpy as np
import pandas as pd
from luigi import six
from os.path import join
from luigi import configuration
from luigi.contrib import postgres
from luigi.s3 import S3Target
from dotenv import find_dotenv
from itertools import product
from politica_preventiva.pipelines.ingest.classic.classic_ingest_tasks import *
from politica_preventiva.pipelines.ingest.classic.\
    preprocessing_scripts.preprocessing_scripts import *
from politica_preventiva.pipelines.utils.pipeline_utils import parse_cfg_list, \
    extras, historical_dates, latest_dates, get_extra_str
from politica_preventiva.pipelines.utils import s3_utils

# Load Postgres Schemas
temp = open('./pipelines/common/pg_raw_schemas.txt').read()
schemas = ast.literal_eval(temp)
open('./pipelines/common/pg_raw_schemas.txt').close()
conf = configuration.get_config()

# AWS
aws_access_key_id = os.environ.get('AWS_ACCESS_KEY_ID')
aws_secret_access_key = os.environ.get('AWS_SECRET_ACCESS_KEY')
PLACES_API_KEY =  os.environ.get('PLACES_API_KEY')


#######################
# Classic Ingest Tasks
#########

class ClassicIngest(luigi.WrapperTask):

    """ Wrapper of Classic Ingestion.

        Note
        ---------
        Define pipelines to run in luigi.cfg [ClassicIngest]
    """
    current_date = luigi.DateParameter()
    # List all pipelines to run
    pipelines = parse_cfg_list(conf.get("ClassicIngest", "pipelines"))

    def requires(self):
        # loop through pipeline tasks
        yield [UpdateDB(current_date=self.current_date,
                        pipeline_task=pipeline) for pipeline in self.pipelines]


class UpdateDB(postgres.CopyToTable):

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
    pipeline_task = luigi.Parameter()
    # extra = luigi.Parameter()
    client = luigi.s3.S3Client()
    local_path = luigi.Parameter('DEFAULT')  # path where csv is located
    raw_bucket = luigi.Parameter('DEFAULT')  # s3 bucket address

    # RDS
    database = os.environ.get("PGDATABASE")
    user = os.environ.get("POSTGRES_USER")
    password = os.environ.get("POSTGRES_PASSWORD")
    host = os.environ.get("PGHOST")

    def requires(self):
        return Concatenation(current_date=self.current_date,
                             pipeline_task=self.pipeline_task)

    # @property # TODO()
    # def update_id(self):
    #   num = str(random.randint(0,100000)) + self.pipeline_task
    #   num = str(self.current_date)  + self.pipeline_task
    #   return num

    @property
    def columns(self):
        return schemas[self.pipeline_task]["SCHEMA"]

    @property
    def table(self):
        return "raw." + self.pipeline_task

    def rows(self):
        # Path of last "ouput" version #TODO(Return to input version)
        output_path = self.input().path
        # output_path = "s3://dpa-plataforma-preventiva/etl/indesol/
        # concatenation/" + \
        # "2017-06" + "--" + self.pipeline_task + ".csv"
        data = pd.read_csv(output_path, sep="|", encoding="utf-8", dtype=str,
                           error_bad_lines=False)
        # data = data.replace(r'\s+', np.nan, regex=True).replace('', np.nan)
        data = data.replace('nan', np.nan, regex=True)
        data = data.where((pd.notnull(data)), None)

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
            raise Exception('columns must consist of column strings\
            or (column string, type string)\
            tuples (was %r ...)' % (self.columns[0],))

        index = schemas[self.pipeline_task]["INDEX"][0]

        # Change the temp setting of the buffer
        # cmd = "SET temp_buffers = 1000MB;"
        # Create temporary table temp
        cmd = "CREATE TEMPORARY TABLE tmp  ({0});ANALYZE tmp;".\
            format(create_sql)
        cmd += 'CREATE INDEX IF NOT EXISTS {0}_index ON\
         tmp ({0});'.format(index, self.pipeline_task)

        cursor.execute(cmd)

        # Copy to TEMP table
        cursor.copy_from(file, "tmp", null=r'\\N', sep=self.column_separator,
                         columns=column_names)

        # Check if raw table exists if not create and build index
        # (defined in common/pg_raw_schemas)
        cmd = "CREATE TABLE IF NOT EXISTS {0}  ({1}) ;".format(self.table,
            create_sql, unique_sql)
        cmd += 'CREATE INDEX IF NOT EXISTS {0}_index ON {1} ({0});'.\
            format(index, self.table)

        # SET except from TEMP to raw table ordered
        cmd += "INSERT INTO {0} \
            SELECT {1} FROM tmp EXCEPT SELECT {1} FROM {0} ;".\
            format(self.table, unique_sql, index)
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
                           "/concatenation/")

    def output(self):
        return postgres.PostgresTarget(host=self.host, database=self.database, 
            user=self.user, password=self.password, table=self.table, 
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
    client = luigi.s3.S3Client()
    raw_bucket = luigi.Parameter('DEFAULT')
    historical = luigi.Parameter('historical')

    def requires(self):

        extra = extras(self.pipeline_task)

        if self.historical:
            dates = historical_dates(self.pipeline_task, self.current_date)
        else:
            dates = latest_dates(self.pipeline_task, self.current_date)
        for extra_p, date in product(extra, dates):
            yield Preprocess(pipeline_task=self.pipeline_task,
                             year_month=str(date),
                             current_date=self.current_date,
                             extra=extra_p)

    def run(self):
        # filepath of the output
        result_filepath =  self.pipeline_task + "/concatenation/" + \
            self.pipeline_task + '.csv'
        # folder to concatenate
        folder_to_concatenate = self.pipeline_task + "/preprocess/"
        # function for appending all .csv files in folder_to_concatenate
        s3_utils.run_concatenation(self.raw_bucket, folder_to_concatenate,
                                   result_filepath, '.csv')
        # Delete files in preprocess
        self.client.remove(self.raw_bucket + folder_to_concatenate)

    def output(self):
        return S3Target(path=self.raw_bucket + self.pipeline_task +
                        "/concatenation/" + self.pipeline_task + '.csv')


class Preprocess(luigi.Task):

    """ Concatenation Task.

        Note
        ---------

        Returns
        ---------
    """
    current_date = luigi.DateParameter()
    pipeline_task = luigi.Parameter()
    year_month = luigi.Parameter()
    extra = luigi.Parameter()
    client = luigi.s3.S3Client()
    raw_bucket = luigi.Parameter('DEFAULT')

    def requires(self):
        # with wrapper_failure(self):
        yield LocalToS3(year_month=self.year_month,
                        pipeline_task=self.pipeline_task,
                        extra=self.extra)

    def run(self):

        extra_h = get_extra_str(self.extra)

        key = self.pipeline_task + "/raw/" + self.year_month + "--" + \
            self.pipeline_task + extra_h + ".csv"
        out_key = "etl/" + self.pipeline_task + "/preprocess/" + \
            self.year_month + "--" + \
            self.pipeline_task + extra_h + ".csv"

        preprocess_tasks = eval(self.pipeline_task + '_prep')
        return preprocess_tasks(year_month=self.year_month,
                                s3_file=key, extra_h=extra_h, out_key=out_key)

    def output(self):
        extra_h = get_extra_str(self.extra)
        return S3Target(path=self.raw_bucket + self.pipeline_task +
            "/preprocess/" + self.year_month + "--" +
            self.pipeline_task + extra_h + ".csv")


class LocalToS3(luigi.Task):

    """ LocalToS3 Task.

        Note
        ---------

        Returns
        ---------

    """

    year_month = luigi.Parameter()
    pipeline_task = luigi.Parameter()
    extra = luigi.Parameter()
    client = luigi.s3.S3Client()
    local_path = luigi.Parameter('DEFAULT')  # path where csv is located
    raw_bucket = luigi.Parameter('DEFAULT')  # s3 bucket address

    def requires(self):
        extra_h = get_extra_str(self.extra)
        local_ingest_file = self.local_path + self.pipeline_task +\
            "/" + self.year_month + "--" + self.pipeline_task +\
             extra_h + ".csv"
        # with wrapper_failure(self):
        task = LocalIngest(pipeline_task=self.pipeline_task,
            year_month=self.year_month, local_ingest_file=local_ingest_file,
            extra=self.extra)
        return task

    def run(self):
        extra_h = get_extra_str(self.extra)
        local_ingest_file = self.local_path + self.pipeline_task +\
            "/" + self.year_month + "--"+self.pipeline_task + extra_h + ".csv"
        try:
            self.client.put(local_path=local_ingest_file,
                            destination_s3_path=self.raw_bucket +
                            self.pipeline_task + "/raw/" +
                            self.year_month + "--" +
                            self.pipeline_task + extra_h + ".csv")
        except (FileNotFoundError):
            print('No file found')

    def output(self):
        extra_h = get_extra_str(self.extra)
        return S3Target(path=self.raw_bucket + self.pipeline_task + "/raw/" +
                        self.year_month + "--" + self.pipeline_task +
                        extra_h + ".csv")


class LocalIngest(luigi.Task):

    """ LocalIngest Task.
        Handler for pipeline_task crawlers

        Note
        ---------
        Assumes that the specific pipeline_task crawler is in folder:
        ingest/classic/source/

    """

    pipeline_task = luigi.Parameter()
    year_month = luigi.Parameter()
    local_ingest_file = luigi.Parameter()
    extra = luigi.Parameter()

    def requires(self):
        classic_tasks = eval(self.pipeline_task)
        # with wrapper_failure(self):
        task = classic_tasks(year_month=self.year_month,
                             pipeline_task=self.pipeline_task,
                             local_ingest_file=self.local_ingest_file,
                             extra=self.extra)
        return task

    def output(self):
        return luigi.LocalTarget(self.local_ingest_file)