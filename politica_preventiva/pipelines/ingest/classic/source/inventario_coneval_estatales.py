#!/usr/bin/env python
# -*- coding: utf-8 -*-
""""
Ingesta de Inventario de Programas y Acciones Estatales
Fuente: CONEVAL

Este script descarga los datos del Inventario de Programas y Acciones Estatales
de CONEVAL para los periodos especificados.
El Inventario de CONEVAL está disponible en una liga como el source_url que se
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
import re
import sys
from urllib.request import urlretrieve
from zipfile import ZipFile

dictionary_bucket = 'dpa-plataforma-preventiva'
dictionary_key = 'commons/metadata/diccionarios/diccionario_inventario_coneval_estatales.csv'

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
    raw_data.rename(columns=headers_dict, inplace=True)
    return(data)


def inventario_coneval_estatales(data_dir, data_date):
    source_url = 'https://www.coneval.org.mx/Evaluacion/IPE/Documents/Archivos_Estados/INVENTARIO_ESTATAL_CONEVAL_{}.zip'.format(data_date)
    local_zipfile = '{}/inventario_{}.zip'.format(data_dir, data_date)
#
    urlretrieve(source_url, local_zipfile)
    zip_ref = ZipFile(local_zipfile, 'r')
    zip_ref.extractall(data_dir)
    zip_ref.close()
#
    excel_filename = fnmatch.filter(os.listdir(data_dir), 'INVENTARIO*.xlsx')[0]
    new_excel_filename = 'inventario_coneval_{}.xlsx'.format(data_date)
    os.rename(os.path.join(data_dir, excel_filename),
            os.path.join(data_dir, new_excel_filename))
#
    data = pd.read_excel(os.path.join(data_dir, new_excel_filename), skiprows = 2, convert_float = False)
    data.drop(list(data.filter(regex = 'Unnamed|Fuente')), axis = 1, inplace = True)
    data.rename(columns = lambda x: re.sub(r'(^[0-9]+\.\s*)(.*)(\s*$)', r'\2', x), inplace = True)
    data.rename(columns = lambda x: x.strip(), inplace = True)
    return(data)


if __name__ == '__main__':
    # Get Arguments from bash.
    parser = argparse.ArgumentParser(description='Download Inventario CONEVAL: Estatales')
    parser.add_argument('--data_date', type=str, help='Data date')
    parser.add_argument('--data_dir', type=str, help='Local data directory')
    parser.add_argument('--local_ingest_file', type=str, help='Local ingest file')
    args = parser.parse_args()
    _data_dir = args.data_dir
    _local_ingest_file = args.local_ingest_file
    _data_date = args.data_date

    print('Downloading data')

    data = inventario_coneval_estatales(data_dir=_data_dir, data_date=_data_date)
    data.replace('\n', '', inplace = True, regex = True)
    data.replace('\|', '-', inplace = True, regex = True)

    response = s3.get_object(Bucket=dictionary_bucket, Key=dictionary_key)
    dictionary = pd.read_csv(io.BytesIO(response['Body'].read()), sep = '|')

    clean_data = replace_header(dictionary, data, clean_headers_col='clean_name', raw_headers_col='raw_name')
    clean_data = clean_data[pd.notnull(data.nb_ent)]
    clean_data['ciclo'] =  clean_data.ciclo.astype(float).astype(int).astype(str)

    clean_data.to_csv(_local_ingest_file, sep = '|', encoding = 'utf-8', index_label = 'id_programa')

    print('Written to: {}'.format(_local_ingest_file))


