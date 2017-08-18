# coding: utf-8

import ast
import boto3
import datetime
import luigi
import logging
import os
import pdb
import psycopg2
import random
import re
import sqlalchemy
import tempfile
import subprocess

from contextlib import contextmanager
from dotenv import load_dotenv,find_dotenv
from itertools import product
from luigi import configuration
from luigi import six, task
from luigi.contrib import postgres
from luigi.s3 import S3Target, S3Client
import numpy as np
from os.path import join, dirname
import pandas as pd

from politica_preventiva.pipelines.ingest.classic.classic_ingest_tasks import *
from politica_preventiva.pipelines.ingest.classic.preprocessing_scripts.preprocessing_scripts import *
from politica_preventiva.pipelines.utils.pipeline_utils import parse_cfg_list, extras, get_extra_str
from politica_preventiva.pipelines.utils.pg_sedesol import parse_cfg_string, download_dir
from politica_preventiva.pipelines.utils.pipeline_utils import s3_to_pandas
from politica_preventiva.pipelines.utils import s3_utils

conf = configuration.get_config()

# AWS
aws_access_key_id = os.environ.get('AWS_ACCESS_KEY_ID')
aws_secret_access_key = os.environ.get('AWS_SECRET_ACCESS_KEY')
PLACES_API_KEY =  os.environ.get('PLACES_API_KEY')

# Logger
logging_conf = configuration.get_config().get("core", "logging_conf_file")
logging.config.fileConfig(logging_conf)
logger = logging.getLogger("dpa-sedesol")

#######################
# Abstract Tasks
#########

class SourceIngestTask(luigi.Task):
    data_date = luigi.Parameter()
    pipeline_task = luigi.Parameter()
    local_ingest_file = luigi.Parameter()
    docker_ingest_file = luigi.Parameter()
    classic_task_scripts = luigi.Parameter('DEFAULT')
    local_path = luigi.Parameter('DEFAULT')
    extra = luigi.Parameter()
    def requires(self):
        logger.info('Luigi is trying to run the source script'+\
                ' of the pipeline_task {0}'.format(self.pipeline_task))

    def output(self):
        return luigi.LocalTarget(self.local_ingest_file)



#######################
# Classic Ingest Tasks
#######

class denue(SourceIngestTask):

    def run(self):

        if not os.path.exists(self.local_path + self.pipeline_task):
            os.makedirs(self.local_path + self.pipeline_task)

        command_list = ['sudo ', self.classic_task_scripts +\
                'denue.sh', self.local_path + \
                '/' + self.pipeline_task, self.local_ingest_file]
        cmd = " ".join(command_list)

        return subprocess.call(cmd, shell=True)



class pub(luigi.Task):

    client = luigi.s3.S3Client()
    data_date = luigi.Parameter()
    pipeline_task = luigi.Parameter()
    local_ingest_file = luigi.Parameter()
    type_script = luigi.Parameter('sh')
    classic_task_scripts = luigi.Parameter('ClassicIngest')
    local_path = luigi.Parameter('DEFAULT')
    raw_bucket = luigi.Parameter('DEFAULT')

    def run(self):
        if not os.path.exists(self.local_path + self.pipeline_task):
            os.makedirs(self.local_path + self.pipeline_task)


        obj = luigi.s3.get_object(Bucket='dpa-compranet',
                Key='etl/'+ self.pipeline_task + \
                "/output/" + self.pipeline_task + ".csv")

        output_db = pd.read_csv(obj['Body'],sep="|",
                error_bad_lines = False, dtype=str, encoding="utf-8")


        return subprocess.call(cmd, shell=True)

    def output(self):

        return luigi.LocalTarget(self.local_ingest_file)


class cuenta_publica_trimestral(SourceIngestTask):

    def run(self):
        if not os.path.exists(self.local_path + self.pipeline_task):
            os.makedirs(self.local_path + self.pipeline_task)

        command_list = ['sudo sh', self.classic_task_scripts +\
                'cuenta_publica_trimestral.sh', self.data_date,
                self.local_path + \
                '/' + self.pipeline_task, self.local_ingest_file]
        cmd = " ".join(command_list)

        return subprocess.call(cmd, shell=True)


class cuenta_publica_anual(SourceIngestTask):

    def run(self):
        if not os.path.exists(self.local_path + self.pipeline_task):
            os.makedirs(self.local_path + self.pipeline_task)

        year = self.data_date.split("-")[0]
        command_list = ['sudo sh', self.classic_task_scripts +\
                'cuenta_publica_anual.sh', self.data_date, self.local_path + \
                '/' + self.pipeline_task, self.local_ingest_file]
        cmd = " ".join(command_list)

        return subprocess.call(cmd, shell=True)


class sagarpa(SourceIngestTask):

    def run(self):
        if not os.path.exists(self.local_path + self.pipeline_task):
            os.makedirs(self.local_path + self.pipeline_task)

        #extra_cmd = self.extra.split('--')
        #cultivo = extra_cmd[0]

        command_list = ['python', self.classic_task_scripts + "sagarpa.py",
                        '--start', self.data_date, '--cult', self.extra,
                        '--output', self.local_ingest_file]
        cmd = " ".join(command_list)
        print(cmd)
        return subprocess.call([cmd], shell=True)


class sagarpa_cierre(SourceIngestTask):

    def run(self):
        if not os.path.exists(self.local_path + self.pipeline_task):
            os.makedirs(self.local_path + self.pipeline_task)
        #extra_cmd = self.extra.split('--')
        #estado = self.extra_cmd[0]

        command_list = ['python', self.classic_task_scripts + "sagarpa.py",
                        '--start', self.data_date, '--estado', self.extra,
                        '--cierre', 'True', '--output', self.local_ingest_file]
        cmd = " ".join(command_list)
        print(cmd)
        return subprocess.call([cmd], shell=True)

class ipc_ciudades(SourceIngestTask):

    def run(self):
        if not os.path.exists(self.local_path + self.pipeline_task):
            os.makedirs(self.local_path + self.pipeline_task)

        cmd = '''
        sudo docker run --rm  -v $PWD:/politica_preventiva\
                -v politica_preventiva_store:/politica_preventiva/data\
            politica_preventiva/python-task python {0}ipc.py\
            --year {1} --output {2}
        '''.format(self.classic_task_scripts, self.data_date,
                self.docker_ingest_file)

        print("***********************************")
        print(os.getcwd())
        print(cmd)
        return subprocess.call(cmd, shell=True)


class segob_snim(SourceIngestTask):

    def run(self):
        if not os.path.exists(self.local_path + self.pipeline_task):
            os.makedirs(self.local_path + self.pipeline_task)

        extra_cmd = self.extra.split('--')
        extra_cmd = extra_cmd[0]
        command_list = ['python', self.classic_task_scripts + 
        "segob_inafed_snim.py", '--data_date', self.data_date,
        '--output', self.local_ingest_file, "--extra", extra_cmd]
        cmd = " ".join(command_list)
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

        command_list = ['python', self.classic_task_scripts + "economia.py",
                        '--start', self.data_date, end_cmd,
                        '--output', self.local_ingest_file]
        cmd = " ".join(command_list)
        print(cmd)
        return subprocess.call([cmd], shell=True)


class precios_frutos(SourceIngestTask):

    def run(self):

        if not os.path.exists(self.local_path + self.pipeline_task):
            os.makedirs(self.local_path + self.pipeline_task)

        extra_cmd = self.extra.split('--')
        mercado = extra_cmd[0]

        command_list = ['python', self.classic_task_scripts +\
                "economia_frutos.py", '--start', self.data_date,
                '--mercado', mercado, '--output',
                self.local_ingest_file]
        cmd = " ".join(command_list)
        print(cmd)
        #pdb.set_trace()
        return subprocess.call([cmd], shell=True)


class distance_to_services(luigi.Task):

    """

    Task que descarga la distancia a servicios básicos de la base de
    Google.
    TODO(Definir keywords dinamicamente)

    """

    client = luigi.s3.S3Client()
    data_date = luigi.Parameter()
    pipeline_task = luigi.Parameter()
    local_ingest_file = luigi.Parameter()

    classic_task_scripts = luigi.Parameter('ClassicIngest')
    local_path = luigi.Parameter('DEFAULT')
    raw_bucket = luigi.Parameter('DEFAULT')

    extra = luigi.Parameter()


    def run(self):

        if not os.path.exists(self.local_path + self.pipeline_task):
            os.makedirs(self.local_path + self.pipeline_task)

        conn = connect_to_db()
        cur = conn.cursor()
        cur.execute("""SELECT cve_muni, latitud, \
                longitud FROM geoms.municipios""")
        rows = pd.DataFrame(cur.fetchall(),columns=["cve_muni","lat","long"])
        rows=rows[:5]

        # ["Hospital","doctor","bus_station","airport","bank", \
        # "gas_station","university","subway_station","police"]
        for keyword in ["Hospital","bank","university","police"]:
            print("looking for nearest {0}".format(keyword))
            vector_dic = rows.apply(lambda x: info_to_google_services(x["lat"],
                x["long"],keyword),axis=1)
            rows[['driving_dist_{0}'.format(keyword),
            'driving_time_{0}'.format(keyword),
            'formatted_address_{0}'.format(keyword),
            'local_phone_number_{0}'.format(keyword),
            'name_{0}'.format(keyword),
            'walking_dist_{0}'.format(keyword),
            'walking_time_{0}'.format(keyword),
            'website_{0}'.format(keyword)]] = pd.DataFrame(list(vector_dic))

        return rows.to_csv(self.output().path,index=False,sep="|")

    def output(self):
        return luigi.LocalTarget(self.local_ingest_file)

class cenapred(SourceIngestTask):
    """
    Task que descarga los datos de cenapred
    Ver classic_task_scripts.cenapred.py para más información
    """
    def run(self):
        if not os.path.exists(self.local_path + self.pipeline_task):
            os.makedirs(self.local_path + self.pipeline_task)

        command_list = ['python', self.classic_task_scripts + "cenapred.py",
                        '--output', self.local_ingest_file]
        cmd = " ".join(command_list)

        return subprocess.call([cmd], shell=True)


class cajeros_banxico(SourceIngestTask):
    """
    Task que descarga los cajeros actualizados de la base de datos Banxico
    Ver classic_task_scripts.cajeros_banxico.py para más información
    """
    def run(self):
        if not os.path.exists(self.local_path + self.pipeline_task):
            os.makedirs(self.local_path + self.pipeline_task)

        command_list = ['python', self.classic_task_scripts +\
                "cajeros_banxico.py",
                '--output', self.local_ingest_file]
        cmd = " ".join(command_list)
        print(cmd)

        return subprocess.call([cmd], shell=True)


class indesol(SourceIngestTask):
    """
    Task que descarga las ong's con clave CLUNI de INDESOL
    Ver classic_task_scripts.indesol.sh para más información
    """
    def run(self):
        if not os.path.exists(self.local_path + self.pipeline_task):
            os.makedirs(self.local_path + self.pipeline_task)

        command_list = ['sudo sh', self.classic_task_scripts + "indesol.sh",
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
        command_list = ['sudo sh', self.classic_task_scripts + "donatarias_sat.sh",
                        self.data_date,
                        self.local_path + self.pipeline_task,
                        self.local_ingest_file]

        cmd = " ".join(command_list)

        return subprocess.call([cmd], shell=True)

class cuaps_sedesol(SourceIngestTask):
    """
    Task que descarga el diccionario de programas CUAPS 
    desarrollado por SEDESOL
    """
    def run(self):
        if not os.path.exists(self.local_path + self.pipeline_task):
            os.makedirs(self.local_path + self.pipeline_task)

        command_list = ['python', self.classic_task_scripts + "cuaps_sedesol.py",
                        '--start', self.data_date,
                        '--output', self.local_ingest_file]
        cmd = " ".join(command_list)
        print(cmd)
        return subprocess.call([cmd], shell=True)

class mir(SourceIngestTask):
    """
    Task que descarga la matriz de indicadores para resultados
    """
    def run(self):
        if not os.path.exists(self.local_path + self.pipeline_task):
            os.makedirs(self.local_path + self.pipeline_task)

        command_list = ['sudo sh', self.classic_task_scripts + "mir.sh",
                        self.local_path + self.pipeline_task,
                        self.local_ingest_file]

        cmd = " ".join(command_list)

        return subprocess.call([cmd], shell=True)


class msd(SourceIngestTask):

    def run(self):
        if not os.path.exists(self.local_path + self.pipeline_task):
            os.makedirs(self.local_path + self.pipeline_task)

        command_list = ['sudo sh', self.classic_task_scripts + "msd.sh",
                        self.local_path + self.pipeline_task,
                        self.local_ingest_file]
        cmd = " ".join(command_list)

        return subprocess.call([cmd], shell=True)

class evals(SourceIngestTask):

    def run(self):
        if not os.path.exists(self.local_path + self.pipeline_task):
            os.makedirs(self.local_path + self.pipeline_task)

        command_list = ['sudo sh', self.classic_task_scripts + "evals.sh",
                        self.local_path + self.pipeline_task,
                        self.local_ingest_file]
        cmd = " ".join(command_list)

        return subprocess.call([cmd], shell=True)

class asm(SourceIngestTask):

    def run(self):
        if not os.path.exists(self.local_path + self.pipeline_task):
            os.makedirs(self.local_path + self.pipeline_task)

        command_list = ['sudo sh', self.classic_task_scripts + "asm.sh",
                        self.local_path + self.pipeline_task,
                        self.local_ingest_file]
        cmd = " ".join(command_list)

        return subprocess.call([cmd], shell=True)





