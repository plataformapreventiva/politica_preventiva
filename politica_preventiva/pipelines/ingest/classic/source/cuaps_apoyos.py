#!/usr/bin/env python

"""
CUAPS: program-level data

Este script ingesta localmente los datos del CUAPS (Cuestionario Único de Aplicación a
Programas) a nivel apoyo. Se asume que se tiene acceso al bucket
sedesol-lab, en el S3 del proyecto, que contiene un archivo Excel llenado por
el personal de la DGGBP a partir de un formulario enviado a través del portal de SISI.

"""
import argparse
import io
import pandas as pd
import os

from boto3 import client

s3 = client('s3')
source_bucket = 'sedesol-lab'
source_key = 'CUAPS-PROGRAMAS/BDCUAPS_APOCOMP_5.xlsx'


def cuaps_apoyos(source_bucket, source_key, int_cols=[]):
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
            replace('\n', '', regex=True). \
            replace('\r', '', regex=True). \
            replace('\r\n', '', regex=True)
    int_cols_full = int_cols + \
    data.filter(regex='TIPO_APOYO_').columns.values.tolist() + \
    data.filter(regex='INDIC_').columns.values.tolist()
    for col in (int_cols_full):
        data[col] = data[col].apply(lambda x: ''
                if pd.isnull(x)
                else int(float(str(x).replace(' ', ''))))
    return(data)


int_cols = ['CSC_ESTATUS_CUAPS_FK', 'TIENE_COMPONENTES', 'ID_COMPONENTE',
        'AP_COMPO', 'TIPO_POB_APO_COD', 'PERIOD_APOYO_COD', 'PERIOD_MONTO_COD',
        'TEM_APOYO', 'APOYO_GEN_PADRON', 'TIPO_PADRON', 'PERIODICIDAD_PADRON',
        'CUENTA_INFO_GEO', 'INSTRU_SOCIOE']

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

    data = cuaps_apoyos(source_bucket, source_key, int_cols)
    data.to_csv(_local_ingest_file,
                sep='|',
                na_rep='',
                index=False,
                encoding='utf-8')
    print('Wrote data to: {}'.format(_local_ingest_file))

