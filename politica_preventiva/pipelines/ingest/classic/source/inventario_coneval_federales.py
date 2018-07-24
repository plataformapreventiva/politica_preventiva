#!/usr/bin/env python
# -*- coding: utf-8 -*-
""""
Ingesta de Inventario de Programas y Acciones Federales
Fuente: CONEVAL

Este script descarga los datos del Inventario de Programas y Acciones Federales
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
dictionary_key = 'commons/metadata/diccionarios/diccionario_inventario_coneval.csv'

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


def inventario_coneval_federales(data_dir, data_date):
    source_url = 'https://www.coneval.org.mx/Evaluacion/IPFE/Documents/Inventarios_Anteriores/Inventario_{}.zip'.format(data_date)
    local_zipfile = '{}/inventario.zip'.format(data_dir)
#
    urlretrieve(source_url, local_zipfile)
    zip_ref = ZipFile(local_zipfile, 'r')
    zip_ref.extractall(data_dir)
    zip_ref.close()
#
    excel_filename = fnmatch.filter(os.listdir(data_dir),
            'Inventario*.xlsx')[0]
    new_excel_filename = 'inventario_coneval_{}.xlsx'.format(data_date)
    os.rename(os.path.join(data_dir, excel_filename),
            os.path.join(data_dir, new_excel_filename))
#
    data = pd.read_excel(os.path.join(data_dir, new_excel_filename), skiprows = 3, convert_float = False)
    data.drop(list(data.filter(regex = 'Unnamed')), axis = 1, inplace = True)
#
# Super-ugly replacements for super-ugly column names
    data.columns.values[47] = 'Definición-POB_POTEN'
    data.columns.values[51] = 'Definición-POB_OBJET'
    data.rename(columns = lambda x: re.sub(r'(\s+)([\.0-9]*)$', r'\2', x), inplace = True)
    pob_poten_names = [(i, re.sub(r'.[0-9]', '', i) + '-POB_POTEN') for i in data.iloc[:,
        44:47].columns.values]
    pob_objet_names = [(i, re.sub(r'.[0-9]', '', i) + '-POB_OBJET') for i in data.iloc[:,
        48:51].columns.values]
    pob_aten_names = [(i, re.sub(r'.[0-9]', '', i) + '-POB_ATEN') for i in data.iloc[:,
        52:56].columns.values]
    eval_dis_names = [(i, re.sub(r'.[0-9]', '', i) + '-EVALDIS') for i in data.iloc[:,
        65:71].columns.values]
    eval_cr_names = [(i, re.sub(r'.[0-9]', '', i) + '-EVALCR') for i in data.iloc[:,
        72:78].columns.values]
    eval_proc_names = [(i, re.sub(r'.[0-9]', '', i) + '-EVALPROC') for i in
            data.iloc[:, 79:85].columns.values]
    eval_des_names = [(i, re.sub(r'.[0-9]', '', i) + '-EVALDES') for i in
            data.iloc[:, 86:92].columns.values]
    eval_ind_names = [(i, re.sub(r'.[0-9]', '', i) + '-EVALIND') for i in
            data.iloc[:, 93:99].columns.values]
    eval_imp_names = [(i, re.sub(r'.[0-9]', '', i) + '-EVALIMP') for i in
            data.iloc[:, 100:106].columns.values]
    eval_otra_names = [(i, re.sub(r'.[0-9]', '', i) + '-EVALOTRA') for i in
            data.iloc[:, 109:115].columns.values]
    pres_names = [(i, i.replace('\n', '')) for i in data.iloc[:, 61:64]]
    new_names = pob_poten_names + pob_objet_names + pob_aten_names + \
    eval_dis_names + eval_cr_names + eval_proc_names + \
    eval_des_names + eval_ind_names + eval_imp_names + eval_otra_names + \
    pres_names
    data.rename(columns = dict(new_names), inplace = True)
    data = data[pd.notnull(data.Ramo)]
    return(data)


if __name__ == '__main__':
    # Get Arguments from bash.
    parser = argparse.ArgumentParser(description='Download Inventario CONEVAL: Federales')
    parser.add_argument('--data_date', type=str, help='Data date')
    parser.add_argument('--data_dir', type=str, help='Local data directory')
    parser.add_argument('--local_ingest_file', type=str, help='Local ingest file')
    args = parser.parse_args()
    _data_dir = args.data_dir
    _local_ingest_file = args.local_ingest_file
    _data_date = args.data_date

    print('Downloading data')

    data = inventario_coneval_federales(data_dir=_data_dir, data_date=_data_date)
    data.replace('\n', '', inplace = True, regex = True)
    data.replace('\|', '-', inplace = True, regex = True)

    response = s3.get_object(Bucket=dictionary_bucket, Key=dictionary_key)
    dictionary = pd.read_csv(io.BytesIO(response['Body'].read()), sep = '|')

    clean_data = replace_header(dictionary, data, clean_headers_col='clean_name', raw_headers_col='raw_name')
    clean_data[['ciclo', 'anio_inicio_prog', 'anio_operacion_prog']] = clean_data[['ciclo', 'anio_inicio_prog', 'anio_operacion_prog']].apply(lambda x: x.astype(float).astype(int).astype(str), axis = 1)
    clean_data['clave_presupuestal'] = clean_data.clave_presupuestal.astype(float).astype(int).astype(str).apply(lambda x: x.zfill(3))
    clean_data['clave_programa'] = clean_data[['modalidad_presupuestal', 'clave_presupuestal']].apply(lambda x: '-'.join(x), axis=1)
    clean_data['id_programa'] = clean_data[['dependencia_acronimo', 'clave_programa']].apply(lambda x: '-'.join(x), axis=1)

    clean_data.to_csv(_local_ingest_file, sep = '|', encoding = 'utf-8', index = False)

    print('Written to: {}'.format(_local_ingest_file))





