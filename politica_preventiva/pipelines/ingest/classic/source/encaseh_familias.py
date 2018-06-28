import os
import re
import pdb
import argparse
import boto3
import urllib.request

import pandas as pd

from dbfread import DBF
from boto3 import client

def download_df(bucket='', key=''):

    conn = client('s3')
    conn.download_file(Bucket = bucket, Key = key, Filename = 'aux.dbf')
    dbf = DBF('aux.dbf')
    data = pd.DataFrame(iter(dbf))
    data2 = data.drop(["_NullFlags"], axis=1)
    
    try:
        os.remove("aux.dbf")
    except OSError:
        pass

    return data2


def regresa_llaves(base, data_date, cproc, formato):
    
    llaves = []

    cadena = base + str(data_date) + '/' + cproc + '' + str(formato) + '/'
    key_veri = cadena + 'veri.dbf'
    key_porti = cadena + 'port_i.dbf'
    key_portc = cadena + 'port_c.dbf'
    key_result = cadena + 'resultados.dbf'

    llaves.extend([key_porti,key_portc,key_veri,key_result])

    return llaves


def append_data(bucket, iterador):

    data = []

    for llave in iterador:
        print('Downloading: {}'.format(llave))
        df = download_df(bucket=bucket, key=llave)
        print(df.shape)
        data.append(df)

    if len(data) >= 1:
        dat_familia = pd.concat(data, axis=1)

    return dat_familia


def get_dataframe(local_ingest_file = '', data_date = '', cproc = ''):
    
    """ 
    Download dbf from S3 bucket as a dataframe.
    Saves Pandas dataframe, if the file is found

    Args:
        bucket (str): string with name of the S3 bucket containing the file
        key (str): S3 file key
        download_path (str): local filename

    """

    #for k in conn.list_objects(Bucket=bucket)['Contents']:
    #    print(k['Key'])


    bucket = 'verificacion-raw'

    if data_date == 2017:

        if cproc == 'Identificacion':

            llave_formato_2016 = regresa_llaves(base='prospera_', data_date=data_date, cproc=cproc, formato='/Formato_2016')
            llave_formato_2017 = regresa_llaves(base='prospera_', data_date=data_date, cproc=cproc, formato='/Formato_2017')

            iterador_16 = iter(llave_formato_2016)
            dat_familia_16 = append_data(bucket, iterador_16)
            iterador_17 = iter(llave_formato_2017)
            dat_familia_17 = append_data(bucket, iterador_17)

            datos = dat_familia_16.append(dat_familia_17)

            datos = datos.loc[:,~datos.columns.duplicated()]
            datos.to_csv(local_ingest_file, sep = '|', encoding = 'utf-8', index=False)

        elif cproc == 'Recertificacion':

            llaves = regresa_llaves(base='prospera_', data_date=data_date, cproc=cproc, formato='/Formato_2016')

            iterador = iter(llaves)
            dat_familia = append_data(bucket, iterador)

            dat_familia = dat_familia.loc[:,~dat_familia.columns.duplicated()]
            dat_familia.to_csv(local_ingest_file, sep = '|', encoding = 'utf-8', index=False)


    elif (data_date == 2015) | (data_date == 2016):

        # Inicializamos lista de llaves
        # bajar también Reevaluación y VPCS (ligeros) si cproc = Recertificación
        if cproc == 'Recertificacion':
            cadena = 'prospera_' + str(data_date) + '/' + '{cproc}' + '/' + '{tab}'

            procesos = ["Recertificacion","Reevaluacion","VPCS"]
            df = pd.DataFrame()
            for cproc in procesos:
                llaves_aux = regresa_llaves(base='prospera_', data_date=data_date, cproc=cproc, formato='')
                iterador = iter(llaves_aux)
                pdb.set_trace()
                dat = append_data(bucket, iterador)
                df.append(dat)

            df = df.loc[:,~df.columns.duplicated()]
            df.to_csv(local_ingest_file, sep = '|', encoding = 'utf-8', index=False)


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description = 'Download PROSPERA verification data')
    parser.add_argument('--data_date', type = int, help = 'Year')
    parser.add_argument('--local_path', type = str, help = 'Local download path')
    parser.add_argument('--local_ingest_file', type = str, help = 'Local ingest file')
    parser.add_argument('--c_tipo_proc', type=str, default='identificacion',help='Tipo de proceso')
    args = parser.parse_args()
    _data_date = args.data_date
    _local_path = args.local_path
    _local_ingest_file = args.local_ingest_file
    _cproc = args.c_tipo_proc
    get_dataframe(local_ingest_file=_local_ingest_file, data_date=_data_date, cproc=_cproc)
