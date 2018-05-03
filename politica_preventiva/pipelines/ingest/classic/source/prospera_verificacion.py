import os
import re
import pdb
import argparse
import boto3
import urllib.request

import pandas as pd

from dbfread import DBF
from boto3 import client

def download_df(bucket='', key=''):

    conn = client('s3')
    conn.download_file(Bucket = bucket, Key = key, Filename = 'aux.dbf')
    dbf = DBF('aux.dbf')
    data = pd.DataFrame(iter(dbf))
    data2 = data.drop(["_NullFlags"], axis=1)
    
    try:
        os.remove("aux.dbf")
    except OSError:
        pass

    return data2


def get_dataframe(local_ingest_file = '', data_date = '', cproc = ''):
    
    """ 
    Download dbf from S3 bucket as a dataframe.
    Saves Pandas dataframe, if the file is found

    Args:
        bucket (str): string with name of the S3 bucket containing the file
        key (str): S3 file key
        download_path (str): local filename

    """

    #for k in conn.list_objects(Bucket=bucket)['Contents']:
    #    print(k['Key'])


    bucket = 'verificacion-raw'

    if _data_date == 2017:
        
        key_16 = 'prospera_' + str(data_date) + '/' + cproc + '/Formato_2016/' + 'veri.dbf'
        key_17 = 'prospera_' + str(data_date) + '/' + cproc + '/Formato_2017/' + 'veri.dbf'

        data_16 = download_df(bucket=bucket, key=key_16)
        data_17 = download_df(bucket=bucket, key=key_17)

        data_final = data_16.append(data_17)
        data_final.to_csv(local_ingest_file, sep = '|', encoding = 'utf-8', index=False)


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description = 'Download PROSPERA verification data')
    parser.add_argument('--data_date', type = int, help = 'Year')
    parser.add_argument('--local_path', type = str, help = 'Local download path')
    parser.add_argument('--local_ingest_file', type = str, help = 'Local ingest file')
    parser.add_argument('--c_tipo_proc', type=str, default='identificacion',
                        help='Tipo de proceso')


    args = parser.parse_args()
    _data_date = args.data_date
    _local_path = args.local_path
    _local_ingest_file = args.local_ingest_file
    _cproc = args.c_tipo_proc
    
    get_dataframe(local_ingest_file=_local_ingest_file, data_date=_data_date, cproc=_cproc)
