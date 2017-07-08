import argparse
from datetime import datetime
import csv
import os
import pdb
import yaml

import pandas as pd


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

    # Load Header dic
    with open(common_path + 'raw_schemas.yaml', 'r') as file:
        header_d = yaml.load(file)

    with open(path, newline='') as f:
            reader = csv.reader(f)
            first_line = next(reader)
    #first_line = data.split('\n', 1)[0]

    if str2bool(new):
        header_d[task]["RAW"] = first_line
        with open(common_path + 'raw_schemas.yaml', 'w') as file:
                yaml.dump(header_d, file, default_flow_style=False)
    else:
        try:
            old_header = header_d[task]["RAW"]
            if old_header != first_line[0].split("|"):
               raise("Error! Header schema has change")
            else:
                pass
        except:
            header_d[task]["RAW"] = first_line[0].split("|")

    with open(common_path + 'raw_schemas.yaml', 'w') as file:
        yaml.dump(header_d, file, default_flow_style=False)

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
        dictionary['actualizacion'] = current_date
        raise Exception("Dictionary of task {0} is not defined,\
         see {1} ".format(pipeline_task, path))

    # Update actualizacion
    dictionary['actualizacion'] = current_date
    dictionary.to_csv(path, index=False, sep="|", encoding="utf-8")
    return dictionary
