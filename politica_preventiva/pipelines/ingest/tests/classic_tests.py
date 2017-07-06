import argparse
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


def header_test(path, task, new=True):
	""""
        This Task tests the header of the new ingested files
        to see if there has been a change in the structure from
        the source.
    """

	# Load Header dic
	with open('./pipelines/common/raw_schemas.yaml', 'r') as file:
	    header_d = yaml.load(file)

	with open(path, newline='') as f:
		reader = csv.reader(f)
		first_line = next(reader)
    #file = open(path,'r')
    #data = file.read()
    #first_line = data.split('\n', 1)[0]

	if str2bool(new):
		header_d[task]["RAW"] = first_line
		with open('./pipelines/common/raw_schemas.yaml', 'w') as file:
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

	with open('./pipelines/common/raw_schemas.yaml', 'w') as file:
		yaml.dump(header_d, file, default_flow_style=False)
