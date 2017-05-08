# coding: utf-8
import subprocess
import os
from os.path import join, dirname
import luigi
from luigi.s3 import S3Target, S3Client
from luigi import configuration
from utils.pipeline_utils import parse_cfg_list

from dotenv import load_dotenv
# Variables de ambiente
path = os.path.abspath('__file__' + "/../../config/")
dotenv_path = join(path, '.env')
load_dotenv(dotenv_path)

# AWS
aws_access_key_id = os.environ.get('AWS_ACCESS_KEY_ID')
aws_secret_access_key = os.environ.get('AWS_SECRET_ACCESS_KEY')


class classic_ingest(luigi.Task):

    client = luigi.s3.S3Client()
    pipeline_task = luigi.Parameter()
    year_month = luigi.Parameter()
    raw_bucket = luigi.Parameter('DEFAULT')
    local_path = luigi.Parameter('DEFAULT')

    def requires(self):
        return local_to_s3(pipeline_task=self.pipeline_task, year_month=self.year_month)

    def run(self):
        # todo() Función aún sin sentido, copia y pega a output
        output_path = self.raw_bucket + self.pipeline_task + \
            "/output/" + self.pipeline_task + ".csv"

        if not self.client.exists(path=output_path):
            self.client.copy(source_path=self.raw_bucket +
                             self.pipeline_task + "/raw/" + self.year_month +
                             "--" + self.pipeline_task + ".csv",
                             destination_path=output_path)

        else:
            self.client.get(path=output_path,
                            destination_local_path=self.local_path + self.pipeline_task + "output.csv")
            self.client.remove(self.raw_bucket + self.pipeline_task +
                               "/output/" + self.pipeline_task + ".csv")
            self.client.put(self.local_path +
                            self.pipeline_task + "output.csv", output_path)
        return True

    def output(self):
        output_path = self.raw_bucket + self.pipeline_task + \
            "/output/" + self.pipeline_task + ".csv"
        return S3Target(path=output_path)


class local_to_s3(luigi.Task):
    """
    Task getting local files into S3 buckets
    """
    year_month = luigi.Parameter()
    # name of task, both scripts and csv will be stored this way
    pipeline_task = luigi.Parameter()
    client = luigi.s3.S3Client()
    local_path = luigi.Parameter('DEFAULT')  # path where csv is located
    raw_bucket = luigi.Parameter('DEFAULT')  # s3 bucket address
    extra = luigi.Parameter()

    def requires(self):
        local_ingest_file = self.local_path + self.pipeline_task + \
            "/" + self.year_month + "--"+self.pipeline_task + "--" + self.extra + ".csv"
        return local_ingest(pipeline_task=self.pipeline_task, year_month=self.year_month, local_ingest_file=local_ingest_file, extra=self.extra)

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

class local_ingest(luigi.Task):
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
    # Las clases específicas definen el tipo de llamada por hacer
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

class economia(luigi.Task):
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
