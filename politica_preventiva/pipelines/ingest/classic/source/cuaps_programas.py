#!/usr/bin/env python

"""
CUAPS: program-level data

Este script ingesta localmente los datos del CUAPS (Cuestionario Único de Aplicación a
Programas) a nivel programa. Se asume que se tiene acceso al bucket
sedesol-lab, en el S3 del proyecto, que contiene un archivo Excel llenado por
el personal de la DGGBP a partir de un formulario enviado a través del portal de SISI.

"""
import argparse
import io
import pandas as pd
import os
import re

from boto3 import client

s3 = client('s3')
source_bucket = 'sedesol-lab'
source_key = 'CUAPS-PROGRAMAS/BDCUAPS_PROGRAMA_8.xlsx'

def concat_extracols(data, columns_to_map={}):
    """
    Function to concatenate all extra columns containing a prefix into a single
    column.
    :param data: The dataframe to modify
    :type data: Pandas dataframe
    :param columns_to_map: Dictionary containing string prefixes
    as keys and max number of columns as integer values
    :type columns_to_map: dict
    """
    for prefix, max_columns in columns_to_map.items():
        prefix_cols = data.filter(regex=prefix).columns.values
        cols_to_keep = prefix_cols[0:max_columns]
        cols_to_drop = prefix_cols[max_columns:]
        extracol = data[cols_to_drop]. \
                    fillna(''). \
                    apply(lambda x: '-'.join(x), axis=1)
        data.insert(loc=(data.columns.get_loc(prefix_cols[(max_columns-1)])+1),
                    column=(prefix + 'OTROS'),
                    value=extracol)
        data.drop(labels=cols_to_drop, axis=1, inplace=True)


def cuaps_programas(source_bucket, source_key, int_cols=[], float_cols=[]):
    """
    Function to download CUAPS data from S3
    :param source_bucket: Name of the AWS S3 Bucket containing the source file
    :type source_bucket: string
    :param source_key: S3 path to the source file
    :type source_key: string
    :param int_cols: List of integer columns
    :type int_cols: list
    :param float_cols: List of float columns
    :type float_cols: list
    """
    response = s3.get_object(Bucket=source_bucket, Key=source_key)
    binary_data = response['Body'].read()
    print('Downloaded data from {}'.format(source_bucket))
    data = pd.read_excel(io.BytesIO(binary_data)). \
            rename(columns=lambda x: x.replace('.', '_')). \
            replace('\n', '', regex=True)
    concat_extracols(data,
        columns_to_map={'OBJ_ESP_PROG_': 10,
        'COP_TIPINT_': 5,
        'COP_NPART': 5,
        'COP_ORDGOB_': 5,
        'COP_TIPART_': 5})
    for col in (int_cols + float_cols):
        print(col)
        if col in int_cols:
            data[col] = data[col].apply(lambda x: ''
                    if pd.isnull(x) or re.sub(r'[^0-9]+', '', str(x)) == ''
                    else int(float(re.sub(r'[^0-9]+', '', str(x)))))
        else:
            data[col] = data[col].apply(lambda x: ''
                    if pd.isnull(x) or re.sub(r'[^0-9]+', '', str(x)) == ''
                    else float(re.sub(r'[^0-9]+', '', str(x))))
    return(data)


int_cols = ['ORDEN_GOB', 'PART_UA', 'TUA_TEL', 'TUA_EXT', 'RP_TEL', 'RP_EXT',
    'EP_TEL', 'EP_EXT', 'ANIO_INICIO_PROG', 'DOC_NORMA', 'DOC_SUPWEB',
    'SITIO_WEB', 'CVE_ENTIDAD_FEDERATIVA', 'CVE_MUNICIPIO', 'POB_POTEN',
    'NUM_POB_OBJET', 'COPARTICIPACION', 'DER_SOCIAL_EDU', 'DER_SOCIAL_SAL',
    'DER_SOCIAL_ALIM', 'DER_SOCIAL_VIV', 'DER_SOCIAL_MAM', 'DER_SOCIAL_TRA',
    'DER_SOCIAL_SEGSOC', 'DER_SOCIAL_NODIS', 'DER_SOCIAL_BECO', 'DER_SOCIAL_NING',
    'TIENE_COMPONENTES']

float_cols = ['PRES_ANTER', 'PRES_VIGEN']

if __name__ == '__main__':
    # Get Arguments from bash.
    parser = argparse.ArgumentParser(description='Download CUAPS data')
    parser.add_argument('--data_date', type=str, help='Data date')
    parser.add_argument('--data_dir', type=str, help='Local download path')
    parser.add_argument('--local_ingest_file', type=str, help='Local ingest file')
    args = parser.parse_args()

    _data_date = args.data_date
    _data_dir = args.data_dir
    _local_ingest_file = args.local_ingest_file

    data = cuaps_programas(source_bucket, source_key, int_cols, float_cols)
    try:
        data.to_csv(_local_ingest_file,
                    sep='|',
                    na_rep='',
                    index=False,
                    encoding='utf-8')
        print('Wrote data to: {}'.format(_local_ingest_file))
    except:
        print('Unable to write local ingest file')
