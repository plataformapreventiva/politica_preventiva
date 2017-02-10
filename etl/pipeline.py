# coding: utf-8
# Run as: python pipeline.py SedesolPipelines --local-scheduler

import logging
import logging.config

import luigi
import luigi.postgres
from luigi import configuration

from utils.pg_sedesol import parse_cfg_string
from utils.run_models import RunModels

logging_conf = configuration.get_config().get("core", "logging_conf_file")
logging.config.fileConfig(logging_conf)
logger = logging.getLogger("sedesol.pipeline")


class SedesolPipelines(luigi.WrapperTask):
    """
    This wrapper task executes several pipelines simultaneously
    """
    conf = configuration.get_config()
    pipelines = parse_cfg_string(conf.get("etl", "pipelines"))

    def requires(self):
        tasks = []
        for pipeline in self.pipelines:
            tasks.append(SedesolPipeline(pipeline))

        return tasks


class SedesolPipeline(luigi.WrapperTask):
    """
    This wrapper task executes a single pipeline

    It expects a string specifying which section of the luigi.cfg file to
    extract configuration options from.
    """
    pipeline_task = luigi.Parameter()

    def requires(self):
        return RunModels(pipeline_task=self.pipeline_task)


if __name__ == "__main__":
    luigi.run()
