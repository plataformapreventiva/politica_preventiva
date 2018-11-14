#!/usr/bin/env python

"""
CUAPS: criterion-level data

Este script ingesta localmente los datos del CUAPS (Cuestionario Único de Aplicación a
Programas) a nivel criterio de focalización. Se asume que se tiene acceso al bucket
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
source_key = 'CUAPS-PROGRAMAS/BDCUAPS_FOCALIZACION_8.xlsx'

def cuaps_criterios(source_bucket, source_key, int_cols=[]):
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
    for col in int_cols:
        data[col] = data[col].apply(lambda x: ''
                if pd.isnull(x) or re.sub(r'[^0-9]+', '', str(x)) == ''
                else int(float(re.sub(r'[^0-9]+', '', str(x)))))
    return(data)


int_cols = ['ID_COMPONENTE', 'ID_APOYO', 'ORDEN',
        'CSC_NIVEL_0', 'PADRE', 'CSC_CONFIGURACION_FOC']

columns_to_pad = ['ID_COMPONENTE', 'ID_APOYO', 'ORDEN', 'PADRE', 'CSC_CONFIGURACION_FOC']

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

    data = cuaps_criterios(source_bucket, source_key, int_cols)
    for col in columns_to_pad:
        data[col] = data[col].apply(lambda x: str(x).zfill(2))

    try:
        data.to_csv(_local_ingest_file,
                    sep='|',
                    na_rep='',
                    index=False,
                    encoding='utf-8')
        print('Wrote data to: {}'.format(_local_ingest_file))
    except:
        print('Unable to write local ingest file')
