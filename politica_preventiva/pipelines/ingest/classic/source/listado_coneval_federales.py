#!/usr/bin/env python
# -*- coding: utf-8 -*-
""""
Ingesta de Listado de Programas y Acciones Federales
Fuente: CONEVAL

Este script descarga los datos del Listado de Programas y Acciones Federales
de CONEVAL para los periodos especificados.
El listado de CONEVAL está disponible en una liga como el source_url que se
especifica en la función principal de este script, y en todos los casos
probados hasta ahora descarga un archivo .zip que contiene un solo archivo de
Excel.
"""

import argparse
from boto3 import client
import fnmatch
import io
import numpy as np
import os
import pandas as pd
import pdb
import re
import sys
from urllib.request import urlretrieve
from zipfile import ZipFile

dictionary_bucket = 'dpa-plataforma-preventiva'
dictionary_key = 'commons/metadata/diccionarios/diccionario_listado_coneval.csv'

s3 = client('s3')

def replace_header(header_data, raw_data, clean_headers_col, raw_headers_col):
    '''
    Cleans raw headers using a pandas dataframe

    :param header_data: Dataframe with columns for raw and clean headers.
    :type header_data: Pandas dataframe.
    :param pipeline_data: Dataframe with raw headers as column names.
    :type header_data: Pandas dataframe.
    :param clean_headers_col: Column name of the column containing clean
    headers.
    :type clean_headers_col: String.
    :param raw_headers_col: Column name of the column containing raw
    headers.
    :type raw_headers_col: String.

    :returns: A dataset containing all rows and columns from pipeline_data, with clean
    headers.
    :rtype: Pandas dataframe.
    '''
    headers_dict = {row[0]: row[1] for index, row in
            header_data[[raw_headers_col, clean_headers_col]].iterrows()}
    data = raw_data.rename(columns=headers_dict)
    return(data)


def listado_coneval_federales(data_dir, data_date):
    source_url = 'https://www.coneval.org.mx/Evaluacion/IPFE/Documents/Inventarios_Anteriores/Listado_{}.zip'.format(data_date)
    local_zipfile = '{}/listado.zip'.format(data_dir)
#
    urlretrieve(source_url, local_zipfile)
    zip_ref = ZipFile(local_zipfile, 'r')
    zip_ref.extractall(data_dir)
    zip_ref.close()
#
    dir_list = [os.path.join(dp, f) for dp, dn, fn in os.walk(os.path.expanduser(data_dir)) for f in fn]
    try:
        excel_filename = fnmatch.filter(dir_list, '*{}*.xlsx'.format(data_date))[0]
    except Exception as e:
        print('Could not find .xlsx file')
#
    new_excel_filename = 'listado_coneval_{}.xlsx'.format(data_date)
    os.rename(os.path.join(data_dir, excel_filename),
            os.path.join(data_dir, new_excel_filename))
    data = pd.read_excel(os.path.join(data_dir, new_excel_filename),
            skiprows = 2, convert_float = False). \
            rename(columns=lambda x: x.strip()). \
            dropna(subset=['Institución']). \
            replace({'\|':'-'}, regex=True)
    os.remove(os.path.join(data_dir, new_excel_filename))
    return(data)


if __name__ == '__main__':
    # Get Arguments from bash.
    parser = argparse.ArgumentParser(description='Download listado CONEVAL: Federales')
    parser.add_argument('--data_date', type=str, help='Data date')
    parser.add_argument('--data_dir', type=str, help='Local data directory')
    parser.add_argument('--local_ingest_file', type=str, help='Local ingest file')
    args = parser.parse_args()
    _data_dir = args.data_dir
    _local_ingest_file = args.local_ingest_file
    _data_date = args.data_date

    print('Downloading data')

    response = s3.get_object(Bucket=dictionary_bucket, Key=dictionary_key)
    dictionary = pd.read_csv(io.BytesIO(response['Body'].read()), sep = '|')
    data = listado_coneval_federales(data_dir=_data_dir, data_date=_data_date)
    clean_data = replace_header(dictionary, data, 'clean_name', 'raw_name')
    for col in ['clave', 'anio_inicio_prog', 'anio_operacion_prog']:
        clean_data[col] = clean_data[col].fillna(0).astype(int)

    clean_data['cd_programa'] = clean_data.modalidad + clean_data.clave.astype(str).str.pad(3, fillchar='0')

    try:
        clean_data.to_csv(_local_ingest_file, sep='|', index=False)
        print('Written to: {}'.format(_local_ingest_file))
    except Exception as e:
        print('Failed to write data to local ingest file')
