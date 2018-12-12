#!/usr/bin/env python

import csv
import logging
import os
import pandas as pd
import pdb
import psycopg2
import yaml

from boto3 import client, resource
from io import StringIO
from luigi import configuration
from sqlalchemy import create_engine

from politica_preventiva.pipelines.utils.pipeline_utils import parse_cfg_list,\
     extras, dates_list, get_extra_str, s3_to_pandas, final_dates

# Logger and Config
conf = configuration.get_config()
logging_conf = configuration.get_config().get("core", "logging_conf_file")

logging.config.fileConfig(logging_conf)
logger = logging.getLogger("dpa-sedesol")

def pull_features_dependencies(task, deps_dictionary):
    """
    This function goes down the dependency tree to recursively
    unpack all dependencies linked to a features task
    """
    task_list = []
    dep_types = [dt for dt in deps_dictionary[task].keys()]
    if 'clean_dependencies' in dep_types:
        task_list = task_list + deps_dictionary[task]['clean_dependencies']

    if 'models_dependencies' in dep_types:
        task_list = task_list + deps_dictionary[task]['models_dependencies']

    if not 'features_dependencies' in dep_types:
        return task_list
    else:
        for f in deps_dictionary[task]['features_dependencies']:
            f_deps = pull_features_dependencies(f, deps_dictionary)
            task_list = task_list + f_deps
        return task_list

def create_order(order_list):
    return {key: i for i, key in enumerate(order_list)}

def get_features_dates(features_task, current_date):
    """
    This function pulls the data_date array of every dependency of the input
    features task and selects the most granular periodicity for running the
    features pipeline
    """
    with open("pipelines/configs/features_dependencies.yaml", "r") as file:
        composition = yaml.load(file)

    task_list = pull_features_dependencies(task=features_task, deps_dictionary=composition)
    dates_list = [final_dates(task, current_date) for task in task_list]
    order = create_order(['d', 'w', 'm', 'bim',
                          'q', 'a', 'b', 'fixed'])
    sorted_list = sorted(dates_list, key=lambda x: order[x[1]])
    return sorted_list[0]

