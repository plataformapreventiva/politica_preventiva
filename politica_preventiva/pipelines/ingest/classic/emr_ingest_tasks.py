# -*- coding: utf-8 -*-
import luigi
import logging
import os
import pdb
import yaml
import tempfile
import boto3

from dotenv import find_dotenv
from luigi import configuration, six
from luigi.contrib import postgres
from luigi.contrib.s3 import S3Target, S3Client

from politica_preventiva.pipelines.utils.pipeline_utils import parse_cfg_list, \
    extras, dates_list, get_extra_str, s3_to_pandas, final_dates
from politica_preventiva.pipelines.utils import s3_utils
from politica_preventiva.pipelines.ingest.tests import classic_tests
from politica_preventiva.tasks.emr_task.emr_task import *
from politica_preventiva.pipelines.utils import emr_tasks

configuration.LuigiConfigParser.add_config_path('/pipelines/configs/luigi.cfg')
conf = configuration.get_config()

# AWS
aws_access_key_id = os.environ.get('AWS_ACCESS_KEY_ID')
aws_secret_access_key = os.environ.get('AWS_SECRET_ACCESS_KEY')
PLACES_API_KEY = os.environ.get('PLACES_API_KEY')

# Logger
logging_conf = configuration.get_config().get("core", "logging_conf_file")
logging.config.fileConfig(logging_conf)
logger = logging.getLogger("dpa-sedesol")

# EMR
with open("pipelines/ingest/common/emr-config.yaml", "r") as file:
        config = yaml.load(file)
        config_emr = config.get("emr")

#######################
# Classic Ingest Tasks
#######################

class pub_agrupacionesEMR(EmrTask):
    current_date = luigi.DateParameter()
    pipeline_task = luigi.Parameter()
    data_date = luigi.Parameter()
    suffix = luigi.Parameter()
    id_name = luigi.Parameter()

    spark_bucket = luigi.Parameter('DEFAULT')
    raw_bucket = luigi.Parameter('DEFAULT')
    classic_task_scripts = luigi.Parameter('DEFAULT')
    common_path = luigi.Parameter('DEFAULT')
    client = S3Client(aws_access_key_id, aws_secret_access_key)

    def requires(self):
        return [InitializeCluster(id_name=self.id_name),
                pub_concatenationEMR(pipeline_task=self.pipeline_task,
                                     current_date=self.current_date,
                                     data_date=self.data_date,
                                     suffix=self.suffix,
                                     id_name=self.id_name)]

    def run(self):
        # Copy script to S3
        ingest_script = self.classic_task_scripts +\
                         self.pipeline_task + ".py"
        ingest_script_output = self.spark_bucket +\
                       self.pipeline_task + '.py'
        self.client.put(ingest_script, ingest_script_output)
        # Copy yaml to S3
        ingest_aux = self.classic_task_scripts + 'pub_agg.yaml'
        ingest_aux_output = self.spark_bucket + 'pub_agg.yaml'
        self.client.put(ingest_aux, ingest_aux_output)

        cluster_path = self.common_path + "emr_id_" + self.id_name + ".txt"
        F = open(cluster_path,'r')
        ClusterId = F.read().replace('\n','')
        copy_steps = self.emr_loader.copy_steps(self.spark_bucket,
                                [self.pipeline_task + '.py', 'pub_agg.yaml'])
        step_name = self.pipeline_task + '_' + self.data_date
        execute_steps = self.emr_loader.execute_step(step_name=step_name,
                                          script_name=self.pipeline_task + '.py',
                                          parameters={'year': self.data_date})
        self.emr_loader.send_pipeline_steps(steps=copy_steps + execute_steps,
                            job_flow_id=ClusterId)
        status = self.emr_loader.poll_until_all_jobs_completed(ClusterId, step_name)
        job_flow_status = self.emr_loader.get_final_status(job_flow_id=ClusterId,
                                                        step_completed=status,
                                                        cluster_path=cluster_path,
                                                        steps_pending=False)
        return job_flow_status

    def output(self):
        file_path = self.raw_bucket + 'pub_agrupaciones/preprocess/' +\
            '{0}/exitoso.txt'.format(self.data_date)
        return S3Target(file_path)


class pub_concatenationEMR(EmrTask):

    current_date = luigi.DateParameter()
    pipeline_task = luigi.Parameter()
    data_date = luigi.Parameter()
    suffix = luigi.Parameter()
    id_name = luigi.Parameter()

    spark_bucket = luigi.Parameter('DEFAULT')
    raw_bucket = luigi.Parameter('DEFAULT')
    common_path = luigi.Parameter('DEFAULT')

    classic_task_scripts = luigi.Parameter('DEFAULT')
    client = S3Client(aws_access_key_id, aws_secret_access_key)

    def requires(self):

        return [InitializeCluster(id_name=self.id_name),
                pub_cleanEMR(pipeline_task=self.pipeline_task,
                             current_date=self.current_date,
                             data_date=self.data_date,
                             suffix=self.suffix,
                             id_name=self.id_name)]

    def run(self):
        # Copy script to S3
        ingest_script = './pipelines/utils/s3_utils.py'
        ingest_script_output = self.spark_bucket + 's3_utils.py'
        self.client.put(ingest_script, ingest_script_output)

        #Concatenate
        result_filepath = 'concatenation/{0}/pub_{0}.csv'.format(self.data_date)
        # folder to concatenate
        folder_to_concatenate = 'clean/{0}/'.format(self.data_date)

        # Getting cluster id and instructions
        cluster_path = self.common_path + "emr_id_" + self.id_name + ".txt"
        F = open(cluster_path,'r')
        ClusterId = F.read().replace('\n','')
        copy_steps = self.emr_loader.copy_steps(self.spark_bucket, ['s3_utils.py'])
        step_name = 'concatenate' + '_' + self.data_date
        execute_steps = self.emr_loader.execute_step(step_name=step_name,
                                          script_name='s3_utils.py',
                                          parameters={'bucket': 'pub-raw',
                                                      'folder': 'clean/{}'.format(self.data_date),
                                                      'output': 'concatenation/{0}/pub_{0}.csv'.format(self.data_date)},
                                         mode='python')
        self.emr_loader.send_pipeline_steps(steps=copy_steps + execute_steps,
                                          job_flow_id=ClusterId)
        status = self.emr_loader.poll_until_all_jobs_completed(ClusterId, step_name)
        job_flow_status = self.emr_loader.get_final_status(job_flow_id=ClusterId,
                                                        step_completed=status,
                                                        cluster_path=cluster_path)

    def output(self):
        file_path = 's3://pub-raw/concatenation/{year}/pub_{year}.csv'.format(year=self.data_date)
        return S3Target(file_path)


class pub_cleanEMR(EmrTask):

    current_date = luigi.DateParameter()
    pipeline_task = luigi.Parameter()
    data_date = luigi.Parameter()
    suffix = luigi.Parameter()
    id_name = luigi.Parameter()

    spark_bucket = luigi.Parameter('DEFAULT')
    raw_bucket = luigi.Parameter('DEFAULT')
    common_path = luigi.Parameter('DEFAULT')

    classic_task_scripts = luigi.Parameter('DEFAULT')
    client = S3Client(aws_access_key_id, aws_secret_access_key)

    def requires(self):
        return InitializeCluster(id_name=self.id_name)

    def run(self):
        # Copy to S3
        ingest_script = self.classic_task_scripts + 'pub_clean.py'
        ingest_script_output = self.spark_bucket + 'pub_clean.py'
        self.client.put(ingest_script, ingest_script_output)

        cluster_path = self.common_path + "emr_id_" + self.id_name + ".txt"
        F = open(cluster_path,'r')
        ClusterId = F.read().replace('\n','')

        copy_steps = self.emr_loader.copy_steps(self.spark_bucket, ['pub_clean.py'])
        step_name = 'pub_clean_' + self.data_date
        execute_steps = self.emr_loader.execute_step(step_name=step_name,
                                          script_name='pub_clean.py',
                                          parameters={'year': self.data_date})

        self.emr_loader.send_pipeline_steps(steps=copy_steps + execute_steps,
                            job_flow_id=ClusterId)
        status = self.emr_loader.poll_until_all_jobs_completed(ClusterId, step_name)
        job_flow_status = self.emr_loader.get_final_status(job_flow_id=ClusterId,
                                                        step_completed=status,
                                                        cluster_path=cluster_path)

    def output(self):
        file_path = 's3://pub-raw/clean/{0}/exitoso.txt'.format(self.data_date)
        return S3Target(file_path)
