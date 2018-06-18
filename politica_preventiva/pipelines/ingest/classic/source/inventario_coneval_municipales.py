#!/usr/bin/env python
# -*- coding: utf-8 -*-
""""
Ingesta de Inventario de Programas y Acciones Municipales
Fuente: CONEVAL

Este script descarga los datos del Inventario de Programas y Acciones
Municipales de CONEVAL para los periodos especificados.

Los inventarios se encuentran divididos por estado, disponibles para descarga
en formato zip en ligas como
https://www.coneval.org.mx/Evaluacion/IPM/Paginas/Estados/nombre_del_estado.aspx

Cada archivo comprimido contiene un solo archivo de tipo xls,
el cual por simplicidad se extrajo, renombró y guardó en
s3://dpa-platforma-preventiva/etl/inventario_coneval_municipales/temp
"""

import argparse
import boto3
import fnmatch
import io
import numpy as np
import pandas as pd
import os
import re
import sys

source_bucket_name = 'dpa-plataforma-preventiva'
source_folder_name = 'etl/inventario_coneval_municipales/temp'


def download_data(source_bucket_name, source_folder_name, data_dir):
    '''
    Downloads all data from a particular folder in AWS S3 to a local directory

    :param source_bucket_name: Bucket name of bucket where data is stored
    :type source_bucket_name: String.
    :param source_folder_name: Key name where all xls data is stored.
    :type source_folder_name: String.
    :param data_dir: Local directory to store downloaded data.
    :type data_dir: String.
    '''
    s3 = boto3.resource('s3')
    source_bucket = s3.Bucket(source_bucket_name)
    for excel_file in source_bucket.objects.filter(Prefix=source_folder_name):
        if 'xls' in excel_file.key:
            source_bucket.download_file(excel_file.key, '{}/{}'.format(data_dir, excel_file.key.split('/')[3]))


def inventario_coneval_municipales(data_dir, data_date):
    '''
    Source ingest function for pipeline task "inventario_coneval_municipales".
    Downloads and concatenates data from S3 to a single pandas dataframe.

    :param data_dir: Local directory to store downloaded data.
    :type data_dir: String.
    :param data_date: Data date of period to download
    :type data_date: String.
    '''
    joint_data = pd.DataFrame()
    files_list = filter(lambda x: x.endswith('xls'), os.listdir(data_dir))
    for excel_file in files_list:
        data = pd.read_excel(os.path.join(data_dir, excel_file), skip = 3)





if __name__ == '__main__':
    # Get Arguments from bash.
    parser = argparse.ArgumentParser(description='Download Inventario CONEVAL:
        Municipales')
    parser.add_argument('--data_date', type=str, help='Data date')
    parser.add_argument('--data_dir', type=str, help='Local data directory')
    parser.add_argument('--local_ingest_file', type=str, help='Local ingest file')
    args = parser.parse_args()
    _data_dir = args.data_dir
    _local_ingest_file = args.local_ingest_file
    _data_date = args.data_date

    print('Downloading data')

    download_data(source_bucket_name, source_folder_name, _data_dir)

    data = inventario_coneval_federales(data_dir=_data_dir, data_date=_data_date)

    clean_data.to_csv(_local_ingest_file, sep = '|', encoding = 'utf-8', index = False)

    print('Written to: {}'.format(_local_ingest_file))

