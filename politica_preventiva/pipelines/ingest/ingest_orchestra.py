#| coding: utf-8
import re
import os
import ast
import luigi
import psycopg2
import pdb
import boto3
import sqlalchemy
import tempfile
import numpy as np
import datetime
import subprocess
import pandas as pd
from luigi import six, task
from os.path import join, dirname
from luigi import configuration
from luigi.contrib import postgres
from luigi.s3 import S3Target, S3Client
from dotenv import load_dotenv,find_dotenv
#from luigi.contrib.postgres import PostgresTarget
from utils.pg_sedesol import parse_cfg_string, download_dir
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

class UpdateDB(postgres.CopyToTable):

    """
        Pipeline Clásico -
        Esta Task toma la versión Output de cada pipeline_task y guarda en
        Postgres una tabla con su nombre para consumo del API.

        ToDo(Dynamic columns -> replace the class variable with a
        runtime-calculated function using a @property declaration)
        https://groups.google.com/forum/#!msg/luigi-user/FA7MdzXS9IE/RCVculxoviIJ
    """

    pipeline_task = luigi.Parameter()
    year_month = luigi.Parameter()

    # RDS Parameters
    database = os.environ.get("PGDATABASE_COMPRANET")
    user = os.environ.get("POSTGRES_USER_COMPRANET")
    password = os.environ.get("POSTGRES_PASSWORD_COMPRANET")
    host = os.environ.get("PGHOST_COMPRANET")

    @property
    def update_id(self):
        num = str(random.randint(0,100000))
        return num + self.pipeline_task

    @property
    def columns(self):
        return schemas[self.pipeline_task]["SCHEMA"]

    @property
    def table(self):
        return "raw." + self.pipeline_task

    def requires(self):

        return UpdateOutput(pipeline_task=self.pipeline_task,
                            year_month=self.year_month)

    def rows(self):

        data = pd.read_csv(self.input().path,sep="|",error_bad_lines = False,encoding="utf-8",dtype=str)
        #data = data.replace(r'\s+',np.nan,regex=True).replace('',np.nan)
        data = data.replace('nan', np.nan, regex=True)
        data = data.where((pd.notnull(data)), None)

        return [tuple(x) for x in data.to_records(index=False)]

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
        self.output().touch(connection)

        # ToDo(Create uniq index and Foreign keys)
        #index= schemas[self.pipeline_task]["INDEX"]
        #cursor.execute('CREATE INDEX {0}_index ON raw.{1} ({0});'.format(index[0], self.pipeline_task))

        connection.commit()
        self.output().touch(connection)
        # Make the changes to the database persistent
        connection.commit()
        connection.close()
        tmp_file.close()

    def output(self):


        return luigi.postgres.PostgresTarget(host=self.host,database=self.database,user=self.user,
                password=self.password,table=self.table,update_id=self.update_id)

class UpdateOutput(luigi.Task):

    """
        Pipeline Clásico -
        Descarga Bash/Python Almacenamiento en S3

        Task Actualiza la versión Output de cada PipelineTask
        comparando con la última en raw.

        ToDo(Spark Version)
    """

    client = luigi.s3.S3Client()
    pipeline_task = luigi.Parameter()
    year_month = luigi.Parameter()
    raw_bucket = luigi.Parameter('DEFAULT')
    local_path = luigi.Parameter('DEFAULT')

    def requires(self):
        return LocalToS3(pipeline_task=self.pipeline_task, year_month=self.year_month)

    def run(self):

        output_path = self.raw_bucket + self.pipeline_task + \
            "/output/" + self.pipeline_task + ".csv"

        input_path = self.raw_bucket + self.pipeline_task + "/raw/" + self.year_month + \
        "--" + self.pipeline_task + ".csv"

        local_ingest_file = self.local_path + "/" +self.pipeline_task + "/" +self.pipeline_task  + ".csv"            

        if not self.client.exists(path=output_path):
            self.client.copy(source_path=self.raw_bucket + self.pipeline_task + "/raw/" + self.year_month + 
                "--" + self.pipeline_task + ".csv", destination_path=output_path)
        else:
            obj = s3.get_object(Bucket='dpa-compranet', Key='etl/'+ self.pipeline_task + \
                "/output/" + self.pipeline_task + ".csv")

            output_db = pd.read_csv(obj['Body'])

            obj = s3.get_object(Bucket='dpa-compranet', Key='etl/'+ self.pipeline_task + \
                 "/raw/" + self.year_month + "--" + self.pipeline_task + ".csv")

            input_db=pd.read_csv(obj['Body'])


            output_db = output_db.append(input_db, ignore_index=True)
            output_db.drop_duplicates(keep='first', inplace=True)

            output_db.to_csv(local_ingest_file)


            self.client.remove(self.raw_bucket + self.pipeline_task +
                               "/output/" + self.pipeline_task + ".csv")
            self.client.put(local_path=local_ingest_file,
                               destination_s3_path=self.raw_bucket + self.pipeline_task + "/raw/" + 
                               self.year_month + "--" +
                               self.pipeline_task + ".csv")

        return True

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
        if len(self.extra) > 0:
            extra_h = "--" + self.extra
        else:
            extra_h = ""
        local_ingest_file = self.local_path + self.pipeline_task + \
            "/" + self.year_month + "--"+ self.pipeline_task + extra_h + ".csv"
        return LocalIngest(pipeline_task=self.pipeline_task, year_month=self.year_month, 
            local_ingest_file=local_ingest_file, extra=self.extra)

    def run(self):
        if len(self.extra) > 0:
            extra_h = "--" + self.extra
        else:
            extra_h = ""
        local_ingest_file = self.local_path + self.pipeline_task + \
            "/" + self.year_month + "--"+self.pipeline_task + extra_h + ".csv"
        return self.client.put(local_path=local_ingest_file,
                               destination_s3_path=self.raw_bucket + self.pipeline_task + "/raw/" + 
                               self.year_month + "--" +
                               self.pipeline_task +  extra_h + ".csv")

    def output(self):
        if len(self.extra) > 0:
            extra_h = "--" + self.extra
        else:
            extra_h = ""
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
        return classic_tasks(year_month=self.year_month,
                             pipeline_task=self.pipeline_task,
                             local_ingest_file=self.local_ingest_file,
                             extra=self.extra)

    def output(self):
        return luigi.LocalTarget(self.local_ingest_file)

#######################
# Classic Ingest Tasks
####### 
# Funciones usadas por el LocalIngest para los pipeline 
# tasks del Pipeline Clásico
#######################

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


class transparencia(luigi.Task):

    # Las clases específicas definen el tipo de llamada por hacer

    year_month = luigi.Parameter()
    pipeline_task = luigi.Parameter()
    local_ingest_file = luigi.Parameter()

    bash_scripts = luigi.Parameter('DEFAULT')
    local_path = luigi.Parameter('DEFAULT')
    extra = luigi.Parameter()

    def run(self):

        command_list = ['sh', self.bash_scripts + 'transparencia.sh',
                        self.local_path + '/' + self.pipeline_task, self.local_ingest_file]
        cmd = " ".join(command_list)

        return subprocess.call(cmd, shell=True)

    def output(self):

        return luigi.LocalTarget(self.local_ingest_file)

class sagarpa(luigi.Task):
    year_month = luigi.Parameter()
    pipeline_task = luigi.Parameter()
    local_ingest_file = luigi.Parameter()

    python_scripts = luigi.Parameter('DEFAULT')
    local_path = luigi.Parameter('DEFAULT')
    extra = luigi.Parameter()

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

    def output(self):
        return luigi.LocalTarget(self.local_ingest_file)

class sagarpa_cierre(luigi.Task):

    year_month = luigi.Parameter()
    pipeline_task = luigi.Parameter()
    local_ingest_file = luigi.Parameter()

    python_scripts = luigi.Parameter('DEFAULT')
    local_path = luigi.Parameter('DEFAULT')
    extra = luigi.Parameter()

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

    def output(self):
        return luigi.LocalTarget(self.local_ingest_file)

class inpc(luigi.Task):

    year_month = luigi.Parameter()
    pipeline_task = luigi.Parameter()
    local_ingest_file = luigi.Parameter()

    python_scripts = luigi.Parameter('DEFAULT')
    local_path = luigi.Parameter('DEFAULT')
    extra = luigi.Parameter('DEFAULT')

    def run(self):
        if not os.path.exists(self.local_path + self.pipeline_task):
            os.makedirs(self.local_path + self.pipeline_task)

        command_list = ['python', self.python_scripts + "inpc.py",
                        "--year", self.year_month,
                        "--output", self.local_ingest_file]
        cmd = " ".join(command_list)

        return subprocess.call(cmd, shell=True)

    def output(self):

        return luigi.LocalTarget(self.local_ingest_file)

class segob(luigi.Task):

    year_month = luigi.Parameter()
    pipeline_task = luigi.Parameter()
    local_ingest_file = luigi.Parameter()

    python_scripts = luigi.Parameter('DEFAULT')
    local_path = luigi.Parameter('DEFAULT')
    extra = luigi.Parameter('DEFAULT')

    def run(self):
        if not os.path.exists(self.local_path + self.pipeline_task):
            os.makedirs(self.local_path + self.pipeline_task)

        command_list = ['python', self.python_scripts + "segob.py",
        '--output', self.local_ingest_file]
        cmd = " ".join(command_list)
        print(cmd)
        return subprocess.call([cmd], shell=True)

    def output(self):
        return luigi.LocalTarget(self.local_ingest_file)

class precios_granos(luigi.Task):

    year_month = luigi.Parameter()
    pipeline_task = luigi.Parameter()
    local_ingest_file = luigi.Parameter()

    python_scripts = luigi.Parameter('DEFAULT')
    local_path = luigi.Parameter('DEFAULT')
    extra = luigi.Parameter('DEFAULT')

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

    def output(self):
        return luigi.LocalTarget(self.local_ingest_file)

class precios_frutos(luigi.Task):

    year_month = luigi.Parameter()
    pipeline_task = luigi.Parameter()
    local_ingest_file = luigi.Parameter()

    python_scripts = luigi.Parameter('DEFAULT')
    local_path = luigi.Parameter('DEFAULT')
    extra = luigi.Parameter()

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

    def output(self):
        return luigi.LocalTarget(self.local_ingest_file)

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

class cenapred(luigi.Task):
    """
    Task que descarga los datos de cenapred
    Ver python_scripts.cenapred.py para más información
    """
    year_month = luigi.Parameter()
    pipeline_task = luigi.Parameter()
    local_ingest_file = luigi.Parameter()

    python_scripts = luigi.Parameter('DEFAULT')
    local_path = luigi.Parameter('DEFAULT')
    extra = luigi.Parameter()

    def run(self):
        if not os.path.exists(self.local_path + self.pipeline_task):
            os.makedirs(self.local_path + self.pipeline_task)

        command_list = ['python', self.python_scripts + "cenapred.py",
                        '--output', self.local_ingest_file]
        cmd = " ".join(command_list)

        return subprocess.call([cmd], shell=True)

    def output(self):
        return luigi.LocalTarget(self.local_ingest_file)


class cajeros_banxico(luigi.Task):
    """
    Task que descarga los cajeros actualizados de la base de datos Banxico
    Ver python_scripts.cajeros_banxico.py para más información
    """
    year_month = luigi.Parameter()
    pipeline_task = luigi.Parameter()
    local_ingest_file = luigi.Parameter()

    python_scripts = luigi.Parameter('DEFAULT')
    local_path = luigi.Parameter('DEFAULT')
    extra = luigi.Parameter('DEFAULT')

    def run(self):
        if not os.path.exists(self.local_path + self.pipeline_task):
            os.makedirs(self.local_path + self.pipeline_task)

        command_list = ['python', self.python_scripts + "cajeros_banxico.py",
                        '--output', self.local_ingest_file]
        cmd = " ".join(command_list)
        print(cmd)

        return subprocess.call([cmd], shell=True)

    def output(self):
        return luigi.LocalTarget(self.local_ingest_file)

class indesol(luigi.Task):
    """
    Task que descarga las ong's con clave CLUNI de INDESOL
    Ver bash_scripts.indesol.sh para más información
    """
    year_month = luigi.Parameter()
    pipeline_task = luigi.Parameter()
    local_ingest_file = luigi.Parameter()

    bash_scripts = luigi.Parameter('DEFAULT')
    local_path = luigi.Parameter('DEFAULT')
    extra = luigi.Parameter('DEFAULT')

    def run(self):
        if not os.path.exists(self.local_path + self.pipeline_task):
            os.makedirs(self.local_path + self.pipeline_task)

        command_list = ['sh', self.bash_scripts + "indesol.sh",
                        self.local_path + self.pipeline_task,
                        self.local_ingest_file]

        cmd = " ".join(command_list)

        return subprocess.call([cmd], shell=True)

    def output(self):
        return luigi.LocalTarget(self.local_ingest_file)

class donatarias_sat(luigi.Task):
    """
    Task que descarga las donatarias autorizadas por SAT cada año
    Ver bash_cripts.
    """
    year_month = luigi.Parameter()
    pipeline_task = luigi.Parameter()
    local_ingest_file = luigi.Parameter()

    bash_scripts = luigi.Parameter('DEFAULT')
    local_path = luigi.Parameter('DEFAULT')
    extra = luigi.Parameter('DEFAULT')

    def run(self):
        if not os.path.exists(self.local_path + self.pipeline_task):
            os.makedirs(self.local_path + self.pipeline_task)

        command_list = ['sh', self.bash_scripts + "donatarias_sat.sh",
                        self.year_month,
                        self.local_path + self.pipeline_task,
                        self.local_ingest_file]

        cmd = " ".join(command_list)

        return subprocess.call([cmd], shell=True)

    def output(self):
        return luigi.LocalTarget(self.local_ingest_file)
