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

def dictionary_test(features_task, dict_path, table_header, dict_header,
        current_date, data_date, suffix, common_bucket, common_key):
    """
    This function updates the dictionary of the features_task in our features
    schema by pulling the table header in order to include new rows, if any
    """
    try:
        local_dictionary = pd.read_csv(dict_path, sep='|', index_col='id')
        header_series = pd.DataFrame(pd.Series(table_header, name='id'))
        dictionary = header_series.join(local_dictionary, how='left', on='id')
        assert True in dictionary['nombre'].isnull(),\
                'Your dictionary is not complete'
        dictionary['actualizacion_sedesol'] = current_date
        dictionary['data_date'] = data_date + '-' + suffix
        dictionary = dictionary.where((pd.notnull(dictionary)), None)
        dictionary.to_csv(dict_path, index=False,
                          sep='|', encoding='utf-8', quoting=csv.QUOTE_NONE)
        csv_buffer = StringIO()
        dictionary.to_csv(csv_buffer,sep='|', quoting=csv.QUOTE_NONE)
        s3_resource = resource('s3')
        s3_resource.Object(common_bucket,
                     common_key + features_task + '_dic.csv').\
                             put(Body=csv_buffer.getvalue())
    except FileNotFoundError:
        dictionary = pd.DataFrame(table_header, columns=['id'])
        dictionary[['nombre', 'tipo', 'fuente', 'metadata']] = \
                pd.DataFrame([['', '', '', '{}']], index=dictionary.index)
        dictionary['actualizacion_sedesol'] = current_date
        dictionary['data_date'] = data_date + '-' + suffix
        dictionary.to_csv(dict_path, index=False,
                          sep='|', encoding = 'utf-8', quoting=csv.QUOTE_NONE)
        logger.info('Dictionary of task {task} is not defined,\
                see {path}'.format(task=features_task, path=dict_path))
        raise NameError('Dictionary of task {0} is not defined,\
                see {1}'.format(features_task, dict_path))
    except Exception as e:
        logger.critical('All fields in name column must be complete')
        print(e)
    return dictionary

def pull_features_dependencies(task, deps_dictionary):
    """
    This function goes down the dependency tree to recursively
    unpack all dependencies linked to a features task
    """
    task_list = []
    dep_types = [dt for dt in deps_dictionary[task].keys()]
    if 'clean_dependencies' in dep_types:
        task_list = task_list + deps_dictionary[task]['clean_dependencies']

    if 'model_dependencies' in dep_types:
        task_list = task_list + deps_dictionary[task]['model_dependencies']

    if not 'features_dependencies' in dep_types:
        return task_list
    else:
        for f in deps_dictionary[task]['features_dependencies']:
            f_deps = pull_features_dependencies(f, deps_dictionary)
            task_list = task_list + f_deps
        return task_list

def create_granularity_order(order_list):
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
    order = create_granularity_order(['d', 'w', 'm', 'bim',
                                      'q', 'a', 'b', 'fixed'])
    sorted_list = sorted(dates_list, key=lambda x: order[x[1]])
    return sorted_list[0]
