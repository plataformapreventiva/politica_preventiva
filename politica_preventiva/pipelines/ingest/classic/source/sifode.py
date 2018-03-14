#!/usr/bin/env python
# -*- coding: utf-8 -*-

import pandas as pd
import csv
import argparse
import smart_open
import tarfile
import boto3
import botocore
import os

def ingest_sifode(data_date='', data_dir='', local_ingest_file=''):
    """
    This function downloads the compressed SIFODE file for each year from S3 as
    well as the headers for all the programs between 2013 and 2017 and builds a
    CSV containing the hole variables ready for concatenation.
    """

    s3 = boto3.resource('s3')

    with smart_open.smart_open('s3://sifode-raw/variables_totales.csv','r') as f:
        reader = csv.reader(f)
        headers = list(reader)

    headers = [item for sublist in headers for item in sublist]

    with smart_open.smart_open('s3://sifode-raw/variables_'+data_date+'.csv', 'r') as f:
        reader = csv.reader(f)
        headers_anio = list(reader)

    headers_anio = [item for sublist in headers_anio for item in sublist]

    s3.Bucket('sifode-raw').download_file('in_'+data_date+'_39_9.tar.gz',data_dir+'/in_'+data_date+'_39_9.tar.gz')

    tar = tarfile.open(data_dir+'/in_'+data_date+'_39_9.tar.gz', "r:gz")
    tar.extractall(path=data_dir)

    df = pd.DataFrame(columns=headers,dtype=object)

    df2 = pd.read_csv(data_dir+'/in_'+data_date+'_39_9.txt', sep='^',\
            chunksize=1000,dtype=object)

    for chunk in df2:
        chunk.columns=headers_anio
        pd.concat([df,chunk],axis=0).to_csv(local_ingest_file,chunksize=1000,sep='|',mode='a',index=False)

    os.remove(data_dir+'/in_'+data_date+'_39_9.txt')
    os.remove(data_dir+'/in_'+data_date+'_39_9.tar.gz')


if __name__ == '__main__':

    parser = argparse.ArgumentParser(description="Ingest SIFODE data")
    parser.add_argument('--data_date', type=str, default='',
                        help='First year to download, as string format yyyy')
    parser.add_argument('--data_dir', type=str, default='/data/sifode/',
                        help='Local path of ingest data')
    parser.add_argument('--local_ingest_file', type=str, default='',
                        help='Name of output file')

    args = parser.parse_args()
    data_date = args.data_date
    data_dir = args.data_dir
    local_ingest_file = args.local_ingest_file

ingest_sifode(data_date=data_date, data_dir=data_dir, local_ingest_file=local_ingest_file)

