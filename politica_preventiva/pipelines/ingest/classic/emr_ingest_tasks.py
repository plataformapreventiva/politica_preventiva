# -*- coding: utf-8 -*-
import luigi
import logging
import os
import yaml
import tempfile
import boto3

from dotenv import find_dotenv
from luigi import configuration, six
from luigi.contrib import postgres
from luigi.contrib.s3 import S3Target, S3Client

from politica_preventiva.pipelines.ingest.tools.ingest_utils import parse_cfg_list, \
    extras, dates_list, get_extra_str, s3_to_pandas, final_dates
from politica_preventiva.pipelines.utils import s3_utils
from politica_preventiva.pipelines.ingest.tests import classic_tests
from politica_preventiva.tasks.pipeline_task import *
from politica_preventiva.pipelines.utils import emr_tasks

conf = configuration.get_config()

# AWS
aws_access_key_id = os.environ.get('AWS_ACCESS_KEY_ID')
aws_secret_access_key = os.environ.get('AWS_SECRET_ACCESS_KEY')
PLACES_API_KEY = os.environ.get('PLACES_API_KEY')

# Logger
logging_conf = configuration.get_config().get("core", "logging_conf_file")
logging.config.fileConfig(logging_conf)
logger = logging.getLogger("dpa-sedesol")


#######################
# Classic Ingest Tasks
#######################

class pub_agregacionesEMR(EmrTask):
    current_date = luigi.DateParameter()
    pipeline_task = luigi.Parameter()
    data_date = luigi.Parameter()
    suffix = luigi.Parameter()
    id_name = luigi.Parameter()

    spark_bucket = luigi.Parameter('DEFAULT')
    raw_bucket = luigi.Parameter('DEFAULT')

    classic_task_scripts = luigi.Parameter('DEFAULT')
    client = S3Client(aws_access_key_id, aws_secret_access_key)

   def requires(self):
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

        # check if pub clean exists
        exists_clean = self.client.exists('pub-raw/concatenation/{year}/pub_{year}.csv'.format(year=self.data_date))

        if exists_clean:
            return InitializeCluster(id_name=self.id_name)
        else:
            return pub_concatenationEMR(pipeline_task=self.pipeline_task,
                                        current_date=self.current_date,
                                        data_date=self.data_date,
                                        suffix=self.suffix,
                                        id_name=self.id_name)

    def run(self):
        F = open(self.input().path,'r')
        ClusterId = F.read().replace('\n','')
        copy_steps = self.copy_steps(self.spark_bucket, [self.pipeline_task + '.py',
                                                         'pub_agg.yaml'])
        step_name = self.pipeline_task + '_' + self.data_date
        execute_steps = self.execute_step(step_name=step_name,
                                          script_name=self.pipeline_task + '.py',
                                          parameters={'year': self.data_date})

        send_pipeline_steps(steps=copy_steps + execute_steps,
                            job_flow_id=ClusterId)
        status = self.emr_loader.get_final_status(ClusterId, step_name)
        self.emr_loader.shutdown_emr_cluster(ClusterId)
        if not status:
            logger.critical('Pub EMR Step did not finish.')

    def output(self):
        file_path = self.raw_bucket + 'pub/preprocess/' +\
            "{0}/exitoso.txt".format(self.data_date)
        return S3Target(file_path)


class pub_concatenationEMR(AddEmrStep):

    current_date = luigi.DateParameter()
    pipeline_task = luigi.Parameter()
    data_date = luigi.Parameter()
    suffix = luigi.Parameter()
    id_name = luigi.Parameter()

    spark_bucket = luigi.Parameter('DEFAULT')
    raw_bucket = luigi.Parameter('DEFAULT')

    classic_task_scripts = luigi.Parameter('DEFAULT')
    client = S3Client(aws_access_key_id, aws_secret_access_key)

    def requieres(self):
        # Copy script to S3
        ingest_script = './pipelines/utils/s3_utils.py'
        ingest_script_output = self.spark_bucket + 's3_utils.py'
        self.client.put(ingest_script, ingest_script_output)
        # check if pub clean exists
        exists_clean = self.client.exists('pub-raw/clean/{year}/exitoso.txt'.format(year=self.data_date))
        if exists_clean:
            return InitializeCluster(id_name=self.id_name)
        else:
            return pub_cleanEMR(pipeline_task=self.pipeline_task,
                                current_date=self.current_date,
                                data_date=self.data_date,
                                suffix=self.suffix,
                                id_name=self.id_name)
    def run(self):
        #Concatenate
        result_filepath = 'concatenation/{year}/pub_{year}.csv'.format(self.data_date)
        # folder to concatenate
        folder_to_concatenate = 'clean/{year}/'.format(self.date_date)

        # Getting cluster id and instructions
        F = open(self.input().path,'r')
        ClusterId = F.read().replace('\n','')
        copy_steps = self.copy_steps(self.spark_bucket, ['s3_utils.py'])
        step_name = 'concatenate' + '_' + self.data_date
        execute_steps = self.execute_step(step_name=step_name,
                                          script_name='s3_utils.py',
                                          parameters={'bucket': 'pub-raw',
                                                      'folder': 'clean/{}'.format(self.data_date),
                                                      'output': 'concatenation/{0}/pub_{0}.csv'.format(self.data_date),
                                                      'suffix': ''})
        send_pipeline_steps(steps=copy_steps + execute_steps,
                            job_flow_id=ClusterId)
        status = self.emr_loader.get_final_status(ClusterId, step_name)
        self.emr_loader.shutdown_emr_cluster(ClusterId)
        if not status:
            logger.critical('Pub EMR Step did not finish.')

    def output(self):
        file_path = 'pub-raw/concatenation/{year}/pub_{year}.csv'.format(year=self.data_date)
        return S3Target(file_path)


class pub_cleanEMR(AddEmrStep):

    current_date = luigi.DateParameter()
    pipeline_task = luigi.Parameter()
    data_date = luigi.Parameter()
    suffix = luigi.Parameter()
    id_name = luigi.Parameter()

    spark_bucket = luigi.Parameter('DEFAULT')
    raw_bucket = luigi.Parameter('DEFAULT')

    classic_task_scripts = luigi.Parameter('DEFAULT')
    client = S3Client(aws_access_key_id, aws_secret_access_key)

    def requires(self):
        ingest_script = self.classic_task_scripts + 'pub_clean.py'
        ingest_script_output = self.spark_bucket + 'pub_clean.py'
        self.client.put(ingest_script, ingest_script_output)
        return InitializeCluster(id_name=self.id_name)

    def run(self):
        F = open(self.input().path,'r')
        ClusterId = F.read().replace('\n','')

        copy_steps = self.copy_steps(self.spark_bucket, ['pub_clean.py'])
        step_name = 'pub_clean_' + self.data_date
        execute_steps = self.execute_step(step_name=step_name,
                                          script_name='pub_clean.py',
                                          parameters={'year': self.data_date})

        send_pipeline_steps(steps=copy_steps + execute_steps,
                            job_flow_id=ClusterId)
        status = self.emr_loader.get_final_status(ClusterId, step_name)
        if not status:
            logger.critical('Pub EMR Step did not finish.')
            self.emr_loader.shutdown_emr_cluster(ClusterId)

    def output(self):
        file_path = 'pub-raw' +\
                "concatenation/{0}/exitoso.txt".format(self.data_date)
        return S3Target(file_path)
