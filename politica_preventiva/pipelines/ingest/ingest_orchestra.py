#| coding: utf-8
import re
import os
import ast
import luigi
import psycopg2
import random
import pdb
import boto3
import sqlalchemy
import tempfile
import numpy as np
import datetime
import subprocess
from contextlib import contextmanager
import pandas as pd
from luigi import six, task
from ingest.preprocessing_scripts.preprocessing_scripts import *
from os.path import join, dirname
from luigi import configuration
from luigi.contrib import postgres
from luigi.s3 import S3Target, S3Client
from dotenv import load_dotenv,find_dotenv
from itertools import product
from utils.pipeline_utils import parse_cfg_list, extras, historical_dates, latest_dates, get_extra_str
from utils.pg_sedesol import parse_cfg_string, download_dir
from utils.pipeline_utils import s3_to_pandas
from utils import s3_utils
#from utils.google_utils import info_to_google_services

# Variables de ambiente
load_dotenv(find_dotenv())

# Load Postgres Schemas
temp = open('./common/pg_raw_schemas.txt').read()
schemas = ast.literal_eval(temp)
open('./common/pg_raw_schemas.txt').close()

# RDS
database = os.environ.get("PGDATABASE")
user = os.environ.get("POSTGRES_USER")
password = os.environ.get("POSTGRES_PASSWORD")
host = os.environ.get("PGHOST")

# AWS
aws_access_key_id = os.environ.get('AWS_ACCESS_KEY_ID')
aws_secret_access_key = os.environ.get('AWS_SECRET_ACCESS_KEY')
PLACES_API_KEY =  os.environ.get('PLACES_API_KEY')

#######################
# Classic Ingest Tasks
#########
# Definición de un Pipeline estandar  -> pipeline_task. 
#######################
@contextmanager
def wrapper_failure(task):
    try:
        yield
    except Exception as e:
        task.trigger_event(luigi.Event.DEPENDENCY_MISSING, task, e)
        #@classmethod 
        #def output(self):
        #    return True
        pass

class UpdateDB(postgres.CopyToTable):

    """
        Pipeline Clásico -
        Esta Task toma la versión Output de cada pipeline_task y guarda en
        Postgres una tabla con su nombre para consumo del API.

        ToDo(Dynamic columns -> replace the class variable with a
        runtime-calculated function using a @property declaration)
        https://groups.google.com/forum/#!msg/luigi-user/FA7MdzXS9IE/RCVculxoviIJ
    """
    current_date = luigi.DateParameter()
    pipeline_task = luigi.Parameter()
    #extra = luigi.Parameter()
    client = luigi.s3.S3Client()
    local_path = luigi.Parameter('DEFAULT')  # path where csv is located
    raw_bucket = luigi.Parameter('DEFAULT')  # s3 bucket address

    #RDS
    database = os.environ.get("PGDATABASE")
    user = os.environ.get("POSTGRES_USER")
    password = os.environ.get("POSTGRES_PASSWORD")
    host = os.environ.get("PGHOST")

    def requires(self):
        return Concatenation(current_date=self.current_date,
                         pipeline_task=self.pipeline_task)

    @property #TODO()
    def update_id(self):
        #num = str(random.randint(0,100000)) + self.pipeline_task
        num = str(self.current_date)  + self.pipeline_task
        return num 

    @property
    def columns(self):
        return schemas[self.pipeline_task]["SCHEMA"]

    @property
    def table(self):
        return "raw." + self.pipeline_task


    def rows(self):
        # Path of last "ouput" version #TODO(Return to input version)
        output_path = self.input().path
        #output_path = "s3://dpa-plataforma-preventiva/etl/indesol/concatenation/" + \
        # "2017-06" + "--" + self.pipeline_task + ".csv"
        data = pd.read_csv(output_path,sep="|", encoding="utf-8",dtype=str)
        #data = data.replace(r'\s+',np.nan,regex=True).replace('',np.nan)
        data = data.replace('nan', np.nan, regex=True)
        data = data.where((pd.notnull(data)), None)

        return [tuple(x) for x in data.to_records(index=False)]

    def copy(self, cursor, file):
        connection = self.output().connect()
        if isinstance(self.columns[0], six.string_types):
            column_names = self.columns
        elif len(self.columns[0]) == 2:
            #Create columns for csv upload
            column_names = [c[0] for c in self.columns]
            create_sql = [join(c[0],' ',c[1]) for c in self.columns]
            #Create string for table creation
            create_sql = ', '.join(create_sql).replace("/","")
            #Create string for Upsert
            unique_sql = ', '.join(column_names)
        else:
            raise Exception('columns must consist of column strings or (column string, type string) tuples (was %r ...)' % (self.columns[0],))

        index= schemas[self.pipeline_task]["INDEX"][0]

        # Create temporary table temp
        cmd = " CREATE TEMPORARY TABLE tmp  ({0});".format(create_sql)
        cmd += 'CREATE INDEX IF NOT EXISTS {0}_index ON tmp ({0});'.format(index, self.pipeline_task)
        cursor.execute(cmd)

        # Copy to TEMP table
        cursor.copy_from(file, "tmp", null=r'\\N', sep=self.column_separator, columns=column_names)

        # Check if raw table exists if not create and build index (defined in common/pg_raw_schemas)
        cmd = "CREATE TABLE IF NOT EXISTS {0}  ({1}) ;".format(self.table,create_sql,unique_sql)
        cmd+='CREATE INDEX IF NOT EXISTS {0}_index ON {1} ({0});'.format(index,self.table)

        # SET except from TEMP to raw table ordered 
        cmd += "INSERT INTO {0} \
            SELECT {1} FROM tmp EXCEPT SELECT {1} FROM {0} \
            ORDER BY {2};".format(self.table,unique_sql,index)
        cursor.execute(cmd)
        connection.commit()
        return True
        

    def run(self):

        if not (self.table and self.columns):
            raise Exception("table and columns need to be specified")

        connection = self.output().connect()
        tmp_dir = luigi.configuration.get_config().get('postgres', 'local-tmp-dir', None)
        tmp_file = tempfile.TemporaryFile(dir=tmp_dir)
        n = 0

        for row in self.rows():
            n += 1
            rowstr = self.column_separator.join(self.map_column(val) for val in row)
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
                if e.pgcode == psycopg2.errorcodes.UNDEFINED_TABLE and attempt == 0:

                    # if first attempt fails with "relation not found", try creating table
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
        # self.client.remove(self.raw_bucket + self.pipeline_task +
        #           "/processing/" + self.pipeline_task + ".csv")

    def output(self):
        return postgres.PostgresTarget(host=self.host,database=self.database,user=self.user,
                password=self.password,table=self.table,update_id=self.update_id)


class Concatenation(luigi.Task):
    current_date = luigi.DateParameter()
    pipeline_task = luigi.Parameter()
    client = luigi.s3.S3Client()
    #raw_bucket = luigi.Parameter('DEFAULT')
    historical = configuration.get_config().getboolean('DEFAULT', 'historical')
    raw_bucket = configuration.get_config().get('DEFAULT', 'raw_bucket')
    
    def requires(self):

        extra = extras(self.pipeline_task)

        if self.historical:
            dates = historical_dates(self.pipeline_task, self.current_date)
        else:
            dates = latest_dates(self.pipeline_task, self.current_date)
        for extra_p, date in product(extra, dates):
            with wrapper_failure(self):
                return Preprocess(pipeline_task=self.pipeline_task,
                           year_month=str(date),
                           current_date=self.current_date,
                           extra=extra_p) 

    def run(self):
        # filepath of the output
        #pdb.set_trace()
        result_filepath =  self.pipeline_task + "/concatenation/" + \
                      self.pipeline_task + '.csv'
        # folder to concatenate
        folder_to_concatenate = self.pipeline_task + "/preprocess/"
        # function for appending all .csv files in folder_to_concatenate 
        s3_utils.run_concatenation(self.raw_bucket, folder_to_concatenate, result_filepath, '.csv')
        # Delete files in preprocess
        self.client.remove(self.raw_bucket + folder_to_concatenate)
        
    
    def output(self):
        return S3Target(path=self.raw_bucket + self.pipeline_task + "/concatenation/" +
                         self.pipeline_task + '.csv')

class Preprocess(luigi.Task):
    current_date = luigi.DateParameter()
    pipeline_task = luigi.Parameter()
    year_month = luigi.Parameter()
    extra = luigi.Parameter()
    client = luigi.s3.S3Client()
    raw_bucket = configuration.get_config().get('DEFAULT', 'raw_bucket')
    #raw_bucket = luigi.Parameter('DEFAULT')

    def requires(self):
        #with wrapper_failure(self):
        task = LocalToS3(year_month=self.year_month,
                         pipeline_task=self.pipeline_task,
                         extra=self.extra)
        return task

    def run(self):
        extra_h = get_extra_str(self.extra)

        key = self.pipeline_task + "/raw/" + self.year_month + "--" +self.pipeline_task + extra_h + ".csv"
        
        preprocess_tasks = eval(self.pipeline_task + '_prep')

        return preprocess_tasks(year_month=self.year_month, s3_file=key, extra_h = extra_h, 
            out_key = 'etl/' + self.pipeline_task +  "/preprocess/" + self.year_month + "--" + 
            self.pipeline_task + extra_h + ".csv")

    def output(self):
        extra_h = get_extra_str(self.extra)
        return S3Target(path=self.raw_bucket + self.pipeline_task + "/preprocess/" +
            self.year_month + "--" + self.pipeline_task + extra_h + ".csv")


class LocalToS3(luigi.Task):

    """
        Pipeline Clásico -
        Almacena datos descargados de cada
        pipeline task de local al S3 asignado al proyecto.
    """

    year_month = luigi.Parameter()
    pipeline_task = luigi.Parameter()
    extra = luigi.Parameter()
    client = luigi.s3.S3Client()
    local_path = luigi.Parameter('DEFAULT')  # path where csv is located
    raw_bucket = luigi.Parameter('DEFAULT')  # s3 bucket address

    def requires(self):
        extra_h = get_extra_str(self.extra)
        local_ingest_file = self.local_path + self.pipeline_task + \
            "/" + self.year_month + "--"+ self.pipeline_task + extra_h + ".csv"
        #with wrapper_failure(self):
        task =  LocalIngest(pipeline_task=self.pipeline_task, year_month=self.year_month, 
            local_ingest_file=local_ingest_file, extra=self.extra)
        return task

    def run(self):
        extra_h = get_extra_str(self.extra)
        local_ingest_file = self.local_path + self.pipeline_task + \
            "/" + self.year_month + "--"+self.pipeline_task + extra_h + ".csv"
        try:
            self.client.put(local_path=local_ingest_file,
                               destination_s3_path = self.raw_bucket + self.pipeline_task + "/raw/" + 
                               self.year_month + "--" +
                               self.pipeline_task +  extra_h + ".csv")
        except (FileNotFoundError):
            print('No file found')

    def output(self):
        extra_h = get_extra_str(self.extra)
        return S3Target(path=self.raw_bucket + self.pipeline_task + "/raw/" +
            self.year_month + "--" +self.pipeline_task + extra_h + ".csv")


class LocalIngest(luigi.Task):

    """
        Pipeline Clásico -
        Esta Task permite separar los procesos del Pipeline Clasico
        y llamar scripts específicos por pipeline Task
    """

    pipeline_task = luigi.Parameter()
    year_month = luigi.Parameter()
    local_ingest_file = luigi.Parameter()
    extra = luigi.Parameter()

    def requires(self):
        classic_tasks = eval(self.pipeline_task)
        #with wrapper_failure(self):
        task = classic_tasks(year_month=self.year_month,
                                 pipeline_task=self.pipeline_task,
                                 local_ingest_file=self.local_ingest_file,
                                 extra=self.extra)
        return task
    def output(self):
        return luigi.LocalTarget(self.local_ingest_file)


#######################
# Classic Ingest Tasks
####### 
# Funciones usadas por el LocalIngest para los pipeline 
# tasks del Pipeline Clásico
#######################
class SourceIngestTask(luigi.Task):
    year_month = luigi.Parameter()
    pipeline_task = luigi.Parameter()
    local_ingest_file = luigi.Parameter()

    bash_scripts = luigi.Parameter('DEFAULT')
    python_scripts = luigi.Parameter('DEFAULT')
    local_path = luigi.Parameter('DEFAULT')
    extra = luigi.Parameter()

    def output(self):
        return luigi.LocalTarget(self.local_ingest_file)


class pub(luigi.Task):

    client = luigi.s3.S3Client()
    year_month = luigi.Parameter()
    pipeline_task = luigi.Parameter()
    local_ingest_file = luigi.Parameter()
    type_script = luigi.Parameter('sh')

    bash_scripts = luigi.Parameter('DEFAULT')
    local_path = luigi.Parameter('DEFAULT')
    raw_bucket = luigi.Parameter('DEFAULT')

    def run(self):

        obj = luigi.s3.get_object(Bucket='dpa-compranet', Key='etl/'+ self.pipeline_task + \
                "/output/" + self.pipeline_task + ".csv")

        output_db = pd.read_csv(obj['Body'],sep="|",error_bad_lines = False, dtype=str, encoding="utf-8")


        return subprocess.call(cmd, shell=True)

    def output(self):

        return luigi.LocalTarget(self.local_ingest_file)


class transparencia(SourceIngestTask):

    def run(self):

        command_list = ['sh', self.bash_scripts + 'transparencia.sh',
                        self.local_path + '/' + self.pipeline_task, self.local_ingest_file]
        cmd = " ".join(command_list)

        return subprocess.call(cmd, shell=True)


class sagarpa(SourceIngestTask):

    def run(self):
        if not os.path.exists(self.local_path + self.pipeline_task):
            os.makedirs(self.local_path + self.pipeline_task)

        extra_cmd = self.extra.split('--')
        cultivo = extra_cmd[0]

        command_list = ['python', self.python_scripts + "sagarpa.py",
                        '--start', self.year_month, '--cult', cultivo,
                        '--output', self.local_ingest_file]
        cmd = " ".join(command_list)
        print(cmd)
        return subprocess.call([cmd], shell=True)


class sagarpa_cierre(SourceIngestTask):

    def run(self):
        if not os.path.exists(self.local_path + self.pipeline_task):
            os.makedirs(self.local_path + self.pipeline_task)
        extra_cmd = self.extra.split('--')
        estado = extra_cmd[0]

        command_list = ['python', self.python_scripts + "sagarpa.py",
                        '--start', self.year_month, '--estado', estado,
                        '--cierre', 'True', '--output', self.local_ingest_file]
        cmd = " ".join(command_list)
        print(cmd)
        return subprocess.call([cmd], shell=True)

class inpc(SourceIngestTask):

    def run(self):
        if not os.path.exists(self.local_path + self.pipeline_task):
            os.makedirs(self.local_path + self.pipeline_task)

        command_list = ['python', self.python_scripts + "inpc.py",
                        "--year", self.year_month,
                        "--output", self.local_ingest_file]
        cmd = " ".join(command_list)

        return subprocess.call(cmd, shell=True)


class segob(SourceIngestTask):

    def run(self):
        if not os.path.exists(self.local_path + self.pipeline_task):
            os.makedirs(self.local_path + self.pipeline_task)

        command_list = ['python', self.python_scripts + "segob.py",
        '--output', self.local_ingest_file]
        cmd = " ".join(command_list)
        print(cmd)
        return subprocess.call([cmd], shell=True)


class precios_granos(SourceIngestTask):

    def run(self):
        if not os.path.exists(self.local_path + self.pipeline_task):
            os.makedirs(self.local_path + self.pipeline_task)

        extra_cmd = self.extra.split('--')
        end_date = extra_cmd[0]
        if end_date:
            end_cmd = " ".join(['--end', end_date])
        else:
            end_cmd = ""

        command_list = ['python', self.python_scripts + "economia.py",
                        '--start', self.year_month, end_cmd,
                        '--output', self.local_ingest_file]
        cmd = " ".join(command_list)
        print(cmd)
        return subprocess.call([cmd], shell=True)


class precios_frutos(SourceIngestTask):

    def run(self):

        if not os.path.exists(self.local_path + self.pipeline_task):
            os.makedirs(self.local_path + self.pipeline_task)

        extra_cmd = self.extra.split('--')
        end_date = extra_cmd[0]

        if end_date:
            end_cmd = " ".join(['--end', end_date])
        else:
            end_cmd = ""


        command_list = ['python', self.python_scripts + "economia.py", '--frutos True',
                        end_cmd, '--output', self.local_ingest_file,
                        self.year_month]
        cmd = " ".join(command_list)

        print(cmd)

        return subprocess.call([cmd], shell=True)


class distance_to_services(luigi.Task):

    """

    Task que descarga la distancia a servicios básicos de la base de
    Google.
    TODO(Definir keywords dinamicamente)

    """

    client = luigi.s3.S3Client()
    year_month = luigi.Parameter()
    pipeline_task = luigi.Parameter()
    local_ingest_file = luigi.Parameter()

    python_scripts = luigi.Parameter('DEFAULT')
    local_path = luigi.Parameter('DEFAULT')
    raw_bucket = luigi.Parameter('DEFAULT')

    extra = luigi.Parameter()


    def run(self):

        if not os.path.exists(self.local_path + self.pipeline_task):
            os.makedirs(self.local_path + self.pipeline_task)

        conn = connect_to_db()
        cur = conn.cursor()
        cur.execute("""SELECT cve_muni, latitud, longitud FROM geoms.municipios""")
        rows = pd.DataFrame(cur.fetchall(),columns=["cve_muni","lat","long"])
        rows=rows[:5]

        # ["Hospital","doctor","bus_station","airport","bank","gas_station","university","subway_station","police"]
        for keyword in ["Hospital","bank","university","police"]:
            print("looking for nearest {0}".format(keyword))
            vector_dic = rows.apply(lambda x: info_to_google_services(x["lat"],x["long"],keyword),axis=1)           
            rows[['driving_dist_{0}'.format(keyword), 'driving_time_{0}'.format(keyword),
            'formatted_address_{0}'.format(keyword),'local_phone_number_{0}'.format(keyword), 
            'name_{0}'.format(keyword), 'walking_dist_{0}'.format(keyword), 'walking_time_{0}'.format(keyword),
            'website_{0}'.format(keyword)]] = pd.DataFrame(list(vector_dic))

        return rows.to_csv(self.output().path,index=False,sep="|")

    def output(self):
        return luigi.LocalTarget(self.local_ingest_file)

class cenapred(SourceIngestTask):
    """
    Task que descarga los datos de cenapred
    Ver python_scripts.cenapred.py para más información
    """
    def run(self):
        if not os.path.exists(self.local_path + self.pipeline_task):
            os.makedirs(self.local_path + self.pipeline_task)

        command_list = ['python', self.python_scripts + "cenapred.py",
                        '--output', self.local_ingest_file]
        cmd = " ".join(command_list)

        return subprocess.call([cmd], shell=True)


class cajeros_banxico(SourceIngestTask):
    """
    Task que descarga los cajeros actualizados de la base de datos Banxico
    Ver python_scripts.cajeros_banxico.py para más información
    """
    def run(self):
        if not os.path.exists(self.local_path + self.pipeline_task):
            os.makedirs(self.local_path + self.pipeline_task)

        command_list = ['python', self.python_scripts + "cajeros_banxico.py",
                        '--output', self.local_ingest_file]
        cmd = " ".join(command_list)
        print(cmd)

        return subprocess.call([cmd], shell=True)


class indesol(SourceIngestTask):
    """
    Task que descarga las ong's con clave CLUNI de INDESOL
    Ver bash_scripts.indesol.sh para más información
    """
    def run(self):
        if not os.path.exists(self.local_path + self.pipeline_task):
            os.makedirs(self.local_path + self.pipeline_task)

        command_list = ['sh', self.bash_scripts + "indesol.sh",
                        self.local_path + self.pipeline_task,
                        self.local_ingest_file]

        cmd = " ".join(command_list)

        return subprocess.call([cmd], shell=True)


class donatarias_sat(SourceIngestTask):
    """
    Task que descarga las donatarias autorizadas por SAT cada año
    Ver bash_cripts.
    """
    def run(self):
        if not os.path.exists(self.local_path + self.pipeline_task):
            os.makedirs(self.local_path + self.pipeline_task)
        command_list = ['sh', self.bash_scripts + "donatarias_sat.sh",
                        self.year_month,
                        self.local_path + self.pipeline_task,
                        self.local_ingest_file]

        cmd = " ".join(command_list)

        return subprocess.call([cmd], shell=True)

@SourceIngestTask.event_handler(luigi.Event.FAILURE)
def mourn_failure(task, exception):
    """Will be called directly after a failed execution
    of `run` on any MyTask subclass
    """
    print(26 * '-!-')
    print("Boo!, {c} failed.  :(".format(c=self.__class__.__name__))
    print(".. with this exception: '{e}'".format(e=str(exception)))
    print(26 * '-!-')


