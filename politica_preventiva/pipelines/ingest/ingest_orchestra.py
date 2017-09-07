# coding: utf-8

import re
import os
import ast
import pdb
import pandas as pd
import luigi
import logging
from luigi import six, task
from luigi import configuration
from luigi.contrib import postgres
from luigi.s3 import S3Target, S3Client
from politica_preventiva.pipelines.ingest.classic.classic_orchestra import ClassicIngest

logging_conf = configuration.get_config().get("core", "logging_conf_file")
logging.config.fileConfig(logging_conf)
logger = logging.getLogger("dpa-sedesol")

# logger
logging_conf = configuration.get_config().get("core", "logging_conf_file")
logging.config.fileConfig(logging_conf)
logger = logging.getLogger("dpa-sedesol")

class IngestPipeline(luigi.WrapperTask):

    """
    This wrapper task executes ingest pipeline
    It expects a list specifying which ingest pipelines to run
    """

    current_date = luigi.DateParameter()

    def requires(self):

        logger.info('Luigi is running the Ingest Pipeline on the date: {0}'.format(
                    self.current_date))
        return ClassicIngest(current_date=self.current_date)
        # yield GeomIngest()
        # yield PUBIngest()
