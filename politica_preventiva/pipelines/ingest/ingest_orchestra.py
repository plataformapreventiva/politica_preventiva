# coding: utf-8
import re
import os
import ast
import luigi
import psycopg2
import boto3
import sqlalchemy
import tempfile
import numpy as np
import datetime
import subprocess
import pandas as pn
from luigi import six, task
from os.path import join, dirname
from luigi import configuration
from luigi.s3 import S3Target, S3Client
from dotenv import load_dotenv,find_dotenv
#from luigi.contrib.postgres import PostgresTarget
from utils.pg_sedesol import parse_cfg_string, download_dir
from utils.google_utils import info_to_google_services

# Variables de ambiente
load_dotenv(find_dotenv())

# Load Postgres Schemas
temp = open('./common/pg_raw_schemas.txt').read()
schemas = ast.literal_eval(temp)

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
            
            output_db = pn.read_csv(obj['Body'])
            
            obj = s3.get_object(Bucket='dpa-compranet', Key='etl/'+ self.pipeline_task + \
                 "/raw/" + self.year_month + "--" + self.pipeline_task + ".csv")

            input_db=pn.read_csv(obj['Body'])


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
        local_ingest_file = self.local_path + self.pipeline_task + \
            "/" + self.year_month + "--"+self.pipeline_task + "--" + self.extra + ".csv"
        return LocalIngest(pipeline_task=self.pipeline_task, year_month=self.year_month, 
            local_ingest_file=local_ingest_file, extra=self.extra)

    def run(self):
        local_ingest_file = self.local_path + self.pipeline_task + \
            "/" + self.year_month + "--"+self.pipeline_task + "--" + self.extra + ".csv"
        return self.client.put(local_path=local_ingest_file,
                               destination_s3_path=self.raw_bucket + self.pipeline_task + "/raw/" + 
                               self.year_month + "--" + 
                               self.pipeline_task +  "--" + self.extra + ".csv")

    def output(self):
        return S3Target(path=self.raw_bucket + self.pipeline_task + "/raw/" + 
            self.year_month + "--" +self.pipeline_task + "--" + self.extra + ".csv")

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
        return classic_tasks(year_month=self.year_month, pipeline_task=self.pipeline_task,
                             local_ingest_file=self.local_ingest_file, extra=self.extra)

    def output(self):
        return luigi.LocalTarget(self.local_ingest_file)

#######################
# Classic Ingest Tasks
####### 
# Funciones usadas por el LocalIngest para los pipeline 
# tasks del Pipeline Clásico
#######################

class transparencia(luigi.Task):

    # Las clases específicas definen el tipo de llamada por hacer

    client = luigi.s3.S3Client()
    year_month = luigi.Parameter()
    pipeline_task = luigi.Parameter()
    local_ingest_file = luigi.Parameter()
    type_script = luigi.Parameter('sh')

    bash_scripts = luigi.Parameter('DEFAULT')
    local_path = luigi.Parameter('DEFAULT')
    raw_bucket = luigi.Parameter('DEFAULT')

    def run(self):
        # Todo() this can be easily dockerized

        cmd = '''
            {}/{}.{}
            '''.format(self.bash_scripts, self.pipeline_task, self.type_script)

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
        start_date = extra_cmd[0]
        estado = extra_cmd[1]

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
    type_script = luigi.Parameter('sh')

    bash_scripts = luigi.Parameter('DEFAULT')
    local_path = luigi.Parameter('DEFAULT')
    raw_bucket = luigi.Parameter('DEFAULT')

    def run(self):
        # Todo() this can be easily dockerized

        cmd = '''
            {}/{}.{}
            '''.format(self.bash_scripts, self.pipeline_task, self.type_script)

        return subprocess.call(cmd, shell=True)

    def output(self):

        return luigi.LocalTarget(self.local_ingest_file)

class segob(luigi.Task):

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

        command_list = ['python', self.python_scripts + "segob.py",
                        self.local_ingest_file] 
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
    extra = luigi.Parameter()

    def run(self):
        if not os.path.exists(self.local_path + self.pipeline_task):
            os.makedirs(self.local_path + self.pipeline_task)
        
        extra_cmd = self.extra.split('--')
        end_date = extra_cmd[0]

        command_list = ['python', self.python_scripts + "economia.py",
                        '--end', end_date, '--output', self.local_ingest_file, 
                        self.year_month] 
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

        conn = psycopg2.connect(dbname=database,user=user,host=host,password=password)
        cur = conn.cursor()
        cur.execute("""SELECTcve_muni, latitud, longitud FROM geoms.municipios""")
        rows = pn.DataFrame(cur.fetchall(),columns=["cve_muni","lat","long"])
        rows=rows[:5]

        # ["Hospital","doctor","bus_station","airport","bank","gas_station","university","subway_station","police"]
        for keyword in ["Hospital","bank","university","police"]:
            print("looking for nearest {0}".format(keyword))
            vector_dic = rows.apply(lambda x: info_to_google_services(x["lat"],x["long"],keyword),axis=1)           
            rows[['driving_dist_{0}'.format(keyword), 'driving_time_{0}'.format(keyword),
            'formatted_address_{0}'.format(keyword),'local_phone_number_{0}'.format(keyword), 
            'name_{0}'.format(keyword), 'walking_dist_{0}'.format(keyword), 'walking_time_{0}'.format(keyword),
            'website_{0}'.format(keyword)]] = pn.DataFrame(list(vector_dic))

        return rows.to_csv(self.output().path,index=False,sep="|")

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

    def run(self):

        if not os.path.exists(self.local_path + self.pipeline_task):
            os.makedirs(self.local_path + self.pipeline_task)
        
        cmd = """
        python {0} cajeros_banxico.py
        """.format(self.python_scripts)

        print(cmd)

        return subprocess.call([cmd], shell=True)

    def output(self):
        return luigi.LocalTarget(self.local_ingest_file)
