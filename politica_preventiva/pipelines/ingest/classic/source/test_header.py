import argparse
import logging
import os
import pdb
import yaml

import pandas as pd

# log = logging.getLogger(__name__)

def str2bool(v):
    if v.lower() in ('yes', 'true', 't', 'y', '1'):
        return True
    elif v.lower() in ('no', 'false', 'f', 'n', '0'):
        return False
    else:
        raise argparse.ArgumentTypeError('Boolean value expected.')


def header_test(path, task, new):

    # Load data from path
    f = open(path)
    data = f.read()
    first_line = data.split('\n', 1)[0]

    # Load raw schema dic
    with open('./pipelines/common/raw_schemas.yaml', 'r') as f:
        doc = yaml.load(f)

    if new:
        try:
            doc[task]["RAW"] = first_line
        except:
            raise Exception('Problem with the schema definition of {0}- \
                see common/raw_schemas'.format(task))
    else:
        try:
            old_header = doc[task]
            if old_header != first_line:
                raise("Error! Data Source schema has change for task: \
                 {0}".format(task))
            else:
                pass
        except:
            doc[task] = first_line


    with open('./pipelines/common/raw_schemas.yaml', 'w') as file:
        yaml.dump(doc, file, default_flow_style=False)
