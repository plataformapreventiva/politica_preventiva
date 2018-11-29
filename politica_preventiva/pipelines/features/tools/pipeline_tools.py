#!/usr/bin/env python

import csv
import logging
import os
import pandas as pd
import pdb
import psycopg2

from boto3 import client, resource
from io import StringIO
from luigi import configuration
from sqlalchemy import create_engine


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

