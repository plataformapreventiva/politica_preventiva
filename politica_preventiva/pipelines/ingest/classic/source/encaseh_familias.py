#!/usr/bin/env python

"""
TODO() Write familia_id as an integer value
"""

import os
import re
import csv
import pdb
import argparse
import boto3
import urllib.request

import pysal as ps
import pandas as pd

from boto3 import client
from simpledbf import Dbf5
from dbfread import DBF
from progress.bar import Bar # sudo pip install progress


def download_df(bucket='', key=''):
    """
    Args:
        dbfile  : DBF file - Input to be imported
        upper   : Condition - If true, make column heads upper case
    """

    conn = client('s3')
    conn.download_file(Bucket = bucket, Key = key, Filename = 'aux.dbf')
    print("Succesfully downloaded file!")

    print("Converting DBF to csv \n\n")
    table = DBF('aux.dbf')

    archivo = key + ".csv"
    archivo = archivo.replace('/','-')

    with open(archivo, "w", encoding='utf-8') as csvfile:
        writer = csv.writer(csvfile, delimiter=',')
        writer.writerow(table.field_names)

        mm = len(table)
        bar = Bar('Processing', max=mm, suffix='%(index)d/%(max)d - %(percent).1f%% - %(eta)ds')
        for record in table:
            writer.writerow(list(record.values()))
            bar.next()
        bar.finish()

    # data = pd.DataFrame(iter(dbf))
    # dbf = Dbf5('aux.dbf')
    # data = dbf.to_dataframe()

    #pdb.set_trace()
    data = pd.read_csv(archivo)
    data2 = data.drop(["_NullFlags"], axis=1)

    try:
        os.remove("aux.dbf")
        os.remove(archivo)
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

    data = pd.DataFrame()

    for llave in iterador:
        print('Downloading: {}'.format(llave))
        df = download_df(bucket=bucket, key=llave)
        print(df.shape)
        if len(data) == 0:
            data = df
        else:
            data = pd.merge(data, df,
                    on =['FOLIO', 'FOLIO_ENCA','FAMILIA_ID'],
                    how = 'outer', suffixes=('', '_remove'))
            cols = [c for c in data.columns if c.lower()[-6:] != 'remove']
            data= data[cols].copy()

    return data


def get_dataframe(local_ingest_file = '', data_date = '', cproc = ''):

    """
    Download dbf from S3 bucket as a dataframe.
    Saves Pandas dataframe, if the file is found

    Args:
        bucket (str): string with name of the S3 bucket containing the file
        key (str): S3 file key
        download_path (str): local filename

    """

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
            procesos = ["Recertificacion","Reevaluacion","VPCS"]
        else:
            procesos = ['Identificacion']

        df = pd.DataFrame()
        for cproc in procesos:
            llaves_aux = regresa_llaves(base='prospera_', data_date=data_date, cproc=cproc, formato='')
            iterador = iter(llaves_aux)
            dat = append_data(bucket, iterador)
            df = df.append(dat, ignore_index=True)

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
