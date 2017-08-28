import argparse
import csv
import logging
import os
import pdb
import re
import sys
import yaml

from luigi import configuration
from datetime import datetime
import pandas as pd

from politica_preventiva.pipelines.utils.string_cleaner_utils import remove_extra_chars

# logger
logging_conf = configuration.get_config().get("core", "logging_conf_file")
logging.config.fileConfig(logging_conf)
logger = logging.getLogger("dpa-sedesol")

def str2bool(v):
    if v.lower() in ('yes', 'true', 't', 'y', '1'):
        return True
    elif v.lower() in ('no', 'false', 'f', 'n', '0'):
        return False
    else:
        raise argparse.ArgumentTypeError('Boolean value expected.')


def header_test(path, task, common_path, suffix, new=True):
    """"
        This Task tests the header of the new ingested files
        to see if there has been a change in the structure from
        the source.
    """
    noalias_dumper = yaml.dumper.SafeDumper
    noalias_dumper.ignore_aliases = lambda self, data: True

    # Load Header dic
    with open(common_path + 'raw_schemas.yaml', 'r') as file:
        header_d = yaml.load(file)

    with open(path, newline='') as f:
        try:
            reader = csv.reader(f, delimiter='|')
            first_lines = next(reader)
            logger.debug('Remember ingest data file must be \n'+\
                '\t\t\t\t\t must be delimited by pipes "|"')
        except:
            logger.critical('Start date is not defined for the pipeline {0}'+\
                    'Luigi will get only the information of the last period'.\
                format(self.pipeline_task))
            sys.exit('\n Error: failed parsing ingest data file.')
    first_line = [remove_extra_chars(x)  for x in first_lines]
    initial_schema = first_line[:] 
    initial_schema.append("actualizacion_sedesol") 
    initial_schema.append("data_date") 
    
    if str2bool(new):
        header_d[task] = {"RAW": first_line,
                          "LUIGI":{'INDEX':None,
                          "SCHEMA": initial_schema}}
        logger.info('After updating the raw_schemas.yaml with the column types' +\
                    '\n\t\t\t\t\t\t you must change the "new" flag to False in luigi.cfg.')
        # TODO put logger 
        logger.critical('\n\n !!! Important message: \n ' +\
                 'Please specify the column types of the pipeline_task '+\
                 task + '\n see: pipelines/ingest/common/raw_schemas.yaml \n' +\
                 "After you've done that please turn change the" +\
                 " 'new' flag to False in Luigi.cfg. If you do not,"+\
                 "luigi will continue overwriting your dictionary \n\n")
    else:
        try:
            old_header = header_d[task]["RAW"]
            if old_header != first_line:
                logger.critical("Error! Header schema has change")
            else:
                pass
        except:
            logger.critical("The dictionary of the task: "+\
                 task + 'is not defined\n' +\
                 'see. pipelines/ingest/common/raw_schemas.yaml \n\n' +\
                 "Turn on the 'new' flag in Luigi.cfg If you want Luigi" +\
                 " to do it for you")
            
    with open(common_path + 'raw_schemas.yaml', 'w') as file:
        yaml.dump(header_d, file, default_flow_style=False, 
                Dumper=noalias_dumper)
        if str2bool(new):
            sys.exit('\n After updating the raw_schemas.yaml with the column types' +\
                    '\n\t\t\t\t\t\t you must change the "new" flag to False in luigi.cfg.\n')
    file = open(path+".done", "w")
    file.close()

def dictionary_test(pipeline_task, path, header_d, dic_header, current_date,
        data_date, suffix):
    """
    This task updates the dictionary of the pipeline_task in raw
    and creates the legacy neo4j nodes 

    # TODO() Check if the data has other date date
    # TODO() Add Neo4j legacy task
    """

    try:
        dictionary = pd.read_csv(path, sep="|")
        assert dictionary["nombre"].isnull().\
                value_counts().index[0]!=True, "error"

    except:
        task_schema = header_d[pipeline_task]["LUIGI"]["SCHEMA"]
        dictionary = pd.DataFrame([task_schema[i].keys() for i 
            in range(len(task_schema))],columns=["id"])
        dic_header.pop(0)
        dictionary = dictionary.reindex(columns=[*dictionary.\
                columns.tolist(), *dic_header], fill_value=None)
        dictionary.to_csv(path, index=False, sep="|", encoding="utf-8")
        dictionary['actualizacion_sedesol'] = current_date
        dictionary['data_date'] = data_date + '-' + suffix
        logger.critical("Dictionary of task {task} is not defined,\
            see {path}".format(task=pipeline_task, path=path))
        raise Exception("Dictionary of task {0} is not defined,\
         see {1} ".format(pipeline_task, path))


    # Update actualizacion
    dictionary['actualizacion_sedesol'] = current_date
    dictionary['data_date'] = data_date + '-' + suffix
    
    dictionary.to_csv(path, index=False, sep="|", encoding="utf-8")
    return dictionary
