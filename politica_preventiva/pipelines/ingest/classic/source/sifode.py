#!/usr/bin/env python
# -*- coding: utf-8 -*-

import pandas as pd
import csv
import io
import argparse
import smart_open
import tarfile
import boto3

def ingest_sifode(start_date, end_date='', output=''):

    with smart_open.smart_open('s3://sifode-raw/variables_totales.csv', \
            'r') as f:
        reader = csv.reader(f)
        headers = list(reader)

    headers = [item for sublist in headers for item in sublist]

    with smart_open.smart_open('s3://sifode-raw/variables'+, 'r') as f:
        reader = csv.reader(f)
        headers_anio = list(reader)

    headers_anio = [item for sublist in headers_anio for item in sublist]

    s3.Bucket('sifode-raw').download_file('in_'+year+'_39_9.tar.gz','in_'+year+'_39_9.tar.gz')

    tar = tarfile.open('in_'+year+'_39_9.tar.gz', "r:gz")
    tar.extractall()

    df = pd.DataFrame(columns=headers_anio)

    df2 = pd.read_csv('in_'+year+'_39_9.txt', sep='^', chunksize=1000)

    pd.concat([df, df2], axis=0).to_csv("concatenado.csv", chunksize=1000)

    for chunk in df2:
        chunk.columns=headers_anio
        pd.concat([df,chunk],axis=0).to_csv(output,chunksize=1000,sep='|')


if __name__ == '__main__':

    parser = argparse.ArgumentParser(description="Ingest SIFODE data")
    parser.add_argument('--start', type=str, default='2013',
        help= 'First year to download, as string format yyyy')
    parser.add_argument('--end', type=str, default='',
        help= 'Last year to download, as string format yyyy. If None, \
        only download the month specified in start')
    parser.add_argument('--output', type=str, default='',
        help = 'Name of outputfile')
    args = parser.parse_args()
    start_date = args.start
    end_date = args.end
    output = args.output

ingest_sifode(start_date=start_date, end_date=end_date, output=output)

