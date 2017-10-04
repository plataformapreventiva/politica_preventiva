# coding: utf-8

import luigi
import logging
import subprocess
import yaml
import os

from politica_preventiva.pipelines.utils import emr_tasks
from luigi import configuration

# Logger
logger = logging.getLogger("dpa-sedesol")


# AWS
aws_access_key_id = os.environ.get('AWS_ACCESS_KEY_ID')
aws_secret_access_key = os.environ.get('AWS_SECRET_ACCESS_KEY')
PLACES_API_KEY = os.environ.get('PLACES_API_KEY')  

# EMR config

with open("pipelines/ingest/common/emr-config.yaml", "r") as file:
    config = yaml.load(file)
config_emr = config.get("emr")


class DockerTask(luigi.Task):

    """
    Task Abstraction to Dockerize tasks

    Note:

    Use:
    Define the @property def cmd(self):
        python **/**.py
        bash **/**.sh

    """

    def run(self):

        logger.info('Luigi is using the dockerized version of the task' +
                    ' {0}'.format(self.pipeline_task))

        cmd_docker = '''
         docker run -it --rm  -v $PWD:/politica_preventiva\
                -v politica_preventiva_store:/data\
           politica_preventiva/task/docker-task {0} > /dev/null
         '''.format(self.cmd)

        out = subprocess.call(cmd_docker, shell=True)
        logger.info(out)


class EmrTask(luigi.Task):

    """
    Abstract EMR Luigi Task
    """
	# Load EMR Configuration
    emr_loader = emr_tasks.EMRLoader(
        aws_access_key=aws_access_key_id,
        aws_secret_access_key=aws_secret_access_key,
        region_name=config_emr.get("region_name"),
        cluster_name=config_emr.get("cluster_name"),
        instance_count=config_emr.get("instance_count"),
        master_instance_type=config_emr.get("master_instance_type"),
        slave_instance_type=config_emr.get("slave_instance_type"),
        key_name=config_emr.get("key_name"),
        subnet_id=config_emr.get("subnet_id"),
        log_uri=config_emr.get("log_uri"),
        software_version=config_emr.get("software_version"),
        script_bucket_name=config_emr.get("script_bucket_name"),
        key=config_emr.get("key"),
        key_value=config_emr.get("key_value"))

class InitializeCluster(EmrTask):
    """
    Luigi Task to initialize  EMR cluster.
    This Task writes an output token to the location designated by the
    created. The Task will fail if the cluster cannot be initialized.
    """
    current_date = luigi.DateParameter()
    pipeline_task = luigi.Parameter()
    data_date = luigi.Parameter()


    def requires(self):
        """
        Create the EMR cluster
        """
        pdb.set_trace()

        emr_response = emr_loader.launch_cluster()
        emr_client = emr_loader.boto_client("emr")
        return emr_client


