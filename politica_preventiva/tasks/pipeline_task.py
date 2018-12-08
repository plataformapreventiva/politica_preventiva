# coding: utf-8

import luigi
import logging
import subprocess
import yaml
import os
import pdb

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

class PgRTask(luigi.Task):

    """
    Task Abstraction to Dockerize Postgres R tasks

    Note:

    Use:
    Define the @property def cmd(self):
        python **/**.py
        bash **/**.sh

    """

    @property
    def autocommit(self):
        return False

    def run(self):

        cmd_docker = '''
            docker run --env-file $PWD/.env\
             -it --rm  -v $PWD:/politica_preventiva\
            -v politica_preventiva_store:/data\
            politica_preventiva/task/r-task {0}
         '''.format(self.cmd, aws_access_key_id,
                 aws_secret_access_key)
        out = subprocess.call(cmd_docker, shell=True)
        logger.info(out)

        # Update marker table
        connection = self.output().connect()
        connection.autocommit = self.autocommit
        self.output().touch(connection)
        # commit and close connection
        connection.commit()
        connection.close()

class RTask(luigi.Task):

    """
    Task Abstraction to Dockerize tasks

    Note:

    Use:
    Define the @property def cmd(self):
        R **/**.py

    """

    def run(self):

        logger.info('Luigi is using the dockerized version of the Rtask' +
                    ' {0}'.format(self.pipeline_task))

        cmd_docker = '''
         docker run --env-file $PWD/.env\
                 -it --rm  -v $PWD:/politica_preventiva\
                -v politica_preventiva_store:/data\
           politica_preventiva/task/r-task {0}
         '''.format(self.cmd)

        out = subprocess.call(cmd_docker, shell=True)
        pdb.set_trace()
        logger.info(out)


class ModelTask(luigi.Task):

    """
    Task Abstraction to Dockerize models
    Note:

    Use:
    Define the @property def cmd(self):
        R **/**.py

    """
    @property
    def autocommit(self):
        return False


    def run(self):

        logger.info('Luigi is using the dockerized version of the model task' +
                    ' {0}'.format(self.model_task))

        cmd_docker = '''
         docker run --env-file $PWD/.env\
                 -it --rm  -v politica_preventiva_store:/data\
            {0} politica_preventiva/task/model-task
            '''.format(self.cmd)
        out = subprocess.call(cmd_docker.strip(), shell=True)
        pdb.set_trace()
        logger.info(out)

        connection = self.output().connect()
        # mark as complete in same transaction
        self.output().touch(connection)

        # commit and clean up
        connection.commit()
        connection.close()

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
         docker run --env-file $PWD/.env\
                -it --rm  -v $PWD:/politica_preventiva\
                -v politica_preventiva_store:/data\
           politica_preventiva/task/docker-task {0} > /dev/null
         '''.format(self.cmd)

        out = subprocess.call(cmd_docker, shell=True)
        logger.info(out)

