import yaml
import os
import argparse
import pandas as pd
import pdb

def str2bool(v):
    if v.lower() in ('yes', 'true', 't', 'y', '1'):
        return True
    elif v.lower() in ('no', 'false', 'f', 'n', '0'):
        return False
    else:
        raise argparse.ArgumentTypeError('Boolean value expected.')


def check_header(path, task, new):
    with open('./pipelines/common/raw_headers.yaml', 'r') as f:
            doc = yaml.load(f)
    f = open(path)
    data = f.read()
    first_line = data.split('\n', 1)[0]

    if new:
        doc[task] = first_line
        with open('./pipelines/common/raw_headers.yaml', 'w') as file:
            yaml.dump(doc, file, default_flow_style=False)

    else:
        try:
            old_header = doc[task]
            if old_header != first_line:
                raise("Error! Header schema has change")
            else:
                pass
        except:
            doc[task] = first_line

    #with open(path,'r') as f:
    #    with open(path+".temp",'w') as f1:
    #        f.next() 
    #        for line in f:
    #            f1.write(line)
    
    # os.rename(path+".temp", path)

    with open('./pipelines/common/raw_headers.yaml', 'w') as file:
        yaml.dump(doc, file, default_flow_style=False)

if __name__=='__main__':

    parser = argparse.ArgumentParser()
    parser.add_argument('--path', type=str,
                    help = 'Path of File')

    parser.add_argument('--task', type=str,
                    help = 'Name of Task')

    parser.add_argument('--new', type=str2bool, 
                    nargs='?', const=True, 
                    help = 'New Pipelines?')

    args = parser.parse_args()
    path = args.path; task = args.task; new = args.new
    
    check_header(path, task, new)

