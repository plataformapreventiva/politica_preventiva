#!/usr/bin/env python
import argparse
import csv
import logging
import os
import pdb
import re
import sys
import yaml

from boto3 import resource
from datetime import datetime
from io import StringIO
from luigi import configuration
import pandas as pd
from messytables import CSVTableSet, type_guess, \
          types_processor, headers_guess, headers_processor, \
          offset_processor, any_tableset, ReadError


from politica_preventiva.pipelines.utils.string_cleaner_utils import remove_extra_chars

# logger
logging_conf = configuration.get_config().get("core", "logging_conf_file")
logging.config.fileConfig(logging_conf)
logger = logging.getLogger("dpa-sedesol")


def dictionary_test(task, dict_path, table_header, dict_header,
                    current_date, data_date, suffix, common_bucket,
                    common_key, allow_joins=False):
    """
    This function updates the task dictionary data with the
    local dictionary file.  If the task is an ingest task, it will look
    for the task table header in the raw headers yaml. If it is a
    features task, it will instead query the database the current features
    table header.

    # Add a more general way of quoting
    # TODO() Check if the data has other data date (ingest)
    # TODO() Add Neo4j legacy task for both ingest and features pipelines
    # TODO() Rethink dictionary schemas to be more general and standardized
    if possible
    """
    try:
        local_dictionary = pd.read_csv(dict_path, sep='|', index_col='id')
        # If the task is a features task, allow to include new variables in
        # dictionary
        if allow_joins:
            header_series = pd.DataFrame(pd.Series(table_header, name='id'))
            dictionary = header_series.join(local_dictionary, how='left', on='id')
        else:
            dictionary = local_dictionary
            dictionary.reset_index(level=0, inplace=True)
        assert True not in dictionary['nombre'].isnull(),\
                'Your dictionary is not complete'
        dictionary['actualizacion_sedesol'] = current_date
        dictionary['data_date'] = data_date + '-' + suffix
        dictionary = dictionary.where((pd.notnull(dictionary)), None)
        dictionary.to_csv(dict_path, sep='|', encoding='utf-8',
                quoting=csv.QUOTE_NONE)
        csv_buffer = StringIO()
        dictionary.to_csv(csv_buffer,sep='|', encoding='utf-8',
                quoting=csv.QUOTE_NONE)
        s3_resource = resource('s3')
        s3_resource.Object(common_bucket, common_key + task + '_dic.csv').\
                           put(Body=csv_buffer.getvalue())
    except FileNotFoundError:
        # Create an empty dataframe with just enough empty cols
        n = len(dict_header) - 1
        dictionary = pd.DataFrame([[x] + [''] * n for x in table_header],
                                  columns=dict_header)
        dictionary['metadata'] = '{}'
        dictionary['actualizacion_sedesol'] = current_date
        dictionary['data_date'] = data_date + '-' + suffix
        dictionary.to_csv(dict_path, index=False,
                          sep='|', encoding = 'utf-8', quoting=csv.QUOTE_NONE)
        logger.info('Dictionary of task {task} is not defined,\
                see {path}'.format(task=features_task, path=dict_path))
        raise AssertionError('Dictionary of task {0} is not defined,\
                            see {1}'.format(features_task, dict_path))
    except Exception as e:
        logger.critical('All fields in name column must be complete')
        print(e)
    return dictionary

