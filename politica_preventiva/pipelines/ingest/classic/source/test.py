#!/usr/bin/env python
# -*- coding: utf-8 -*-

import csv
import argparse
import boto3
import botocore
import os

def ingest_prueba(data_date='', data_dir='', local_ingest_file=''):
    """
    This is a function to download and ingest a dummy csv
    for pipeline testing
    """

    s3 = boto3.resource('s3')

    s3.Bucket('dpa-plataforma-preventiva').download_file('data/user/ollin18/prueba.csv',local_ingest_file)


if __name__ == '__main__':

    parser = argparse.ArgumentParser(description="Ingest test data")
    parser.add_argument('--data_date', type=str, default='',
                        help='First year to download, as string format yyyy')
    parser.add_argument('--data_dir', type=str, default='/data/test/',
                        help='Local path of ingest data')
    parser.add_argument('--local_ingest_file', type=str, default='',
                        help='Name of output file')

    args = parser.parse_args()
    data_date = args.data_date
    data_dir = args.data_dir
    local_ingest_file = args.local_ingest_file

ingest_prueba(data_date=data_date, data_dir=data_dir, local_ingest_file=local_ingest_file)

