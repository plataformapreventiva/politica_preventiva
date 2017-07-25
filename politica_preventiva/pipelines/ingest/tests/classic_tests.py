import argparse
from datetime import datetime
import csv
import os
import pdb
import re
import yaml

import pandas as pd

from politica_preventiva.pipelines.utils.string_cleaner_utils import remove_extra_chars

def str2bool(v):
    if v.lower() in ('yes', 'true', 't', 'y', '1'):
        return True
    elif v.lower() in ('no', 'false', 'f', 'n', '0'):
        return False
    else:
        raise argparse.ArgumentTypeError('Boolean value expected.')


def header_test(path, task, common_path, new=True):
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
            reader = csv.reader(f, delimiter='|')
            first_lines = next(reader)
    first_line = [remove_extra_chars(x)  for x in first_lines]
    initial_schema = first_line[:] 
    initial_schema.append("actualizacion_sedesol") 
    if str2bool(new):
        header_d[task] = {"RAW": first_line,
                    "LUIGI":{'INDEX':None,
                        "SCHEMA": initial_schema}}
    else:
        try:
            old_header = header_d[task]["RAW"]
            if old_header != first_line:
               raise("Error! Header schema has change")
            else:
                pass
        except:
            header_d[task] = {"RAW": first_line,
                    "LUIGI":{'INDEX':None,
                        "SCHEMA": initial_schema}}
            
    with open(common_path + 'raw_schemas.yaml', 'w') as file:
        yaml.dump(header_d, file, default_flow_style=False, 
                Dumper=noalias_dumper)
    file = open(path+".done", "w")
    file.close()

def dictionary_test(pipeline_task, path, header_d, dic_header, current_date):
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
        raise Exception("Dictionary of task {0} is not defined,\
         see {1} ".format(pipeline_task, path))

    # Update actualizacion
    dictionary['actualizacion_sedesol'] = current_date
    dictionary.to_csv(path, index=False, sep="|", encoding="utf-8")
    return dictionary
