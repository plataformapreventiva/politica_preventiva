# coding: utf-8

import luigi
import logging
import subprocess

from luigi import configuration

# Logger

logging_conf = configuration.get_config().get("core", "logging_conf_file")
logging.config.fileConfig(logging_conf)
logger = logging.getLogger("dpa-sedesol")


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

