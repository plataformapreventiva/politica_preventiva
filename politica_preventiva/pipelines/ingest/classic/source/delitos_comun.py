#!/usr/bin/env python
"""
Ingesta de datos de delitos del fuero común.
Fuente: SESNP

Este script descarga los datos de Delitos del Fuero Común, a nivel municipal.
Los datos están disponibles en una liga indicada por el source_url de este
script, el cual descarga un archivo comprimido .zip
"""

import argparse
import fnmatch
import numpy as np
import os
import pandas as pd

from pyxlsb import open_workbook
from urllib.request import urlretrieve
from zipfile import ZipFile

def source_delitos(data_dir, data_date):
    '''
    Imports data from SENSP
    :param data_date: pipeline task data date, written in "year (4
    digits)"-"month" format
    :type data_date: String
    :param data_dir: Local directory to store data into
    :type data_dir: String
    '''
    start_year = '2015'
    end_year = '2018'
    (year, month) = data_date.split('-')
    if int(year) > int(end_year) or int(year) < int(start_year):
        print('Data date is not contained in time interval of source')
        return

    try:
        source_url = 'http://secretariadoejecutivo.gob.mx/docs/pdfs/nueva-metodologia/Municipal-Delitos-{0}-{1}.zip'.format(start_year, end_year)
        local_zipfile = '{0}/data_sensp_{1}.zip'.format(data_dir, data_date)
        urlretrieve(source_url, local_zipfile)
    except Exception as e:
        print('Unable to download data from remote source')

    try:
        zip_ref = ZipFile(local_zipfile, 'r')
        zip_ref.extractall(data_dir)
        zip_ref.close()
    except Exception as e:
        print('Unable to extract zip file from downloaded source')

    try:
        dir_list = [os.path.join(dp, f) for dp, dn, fn in
            os.walk(os.path.expanduser(data_dir)) for f in fn]
        old_filename = fnmatch.filter(dir_list, '*.xlsb')[0]
    except Exception as e:
        print('Could not find xlsb file')

    new_filename = os.path.join(data_dir,
            'data_sensp_{}.xlsb'.format(data_date))
    os.rename(os.path.join(data_dir, old_filename), new_filename)
    data_rows = []
    with open_workbook(new_filename) as wb:
        with wb.get_sheet(1) as sheet:
            for row in sheet.rows():
                data_rows.append([item.v for item in row])
    data = pd.DataFrame(data_rows[1:], columns=data_rows[0])
    # If there is at least one state with missing data for that month, return no data
    month_cols = {'1': 'Enero', '2': 'Febrero', '3': 'Marzo',
                '4': 'Abril', '5': 'Mayo', '6': 'Junio',
                '7': 'Julio', '8': 'Agosto', '9': 'Septiembre',
                '10': 'Octubre', '11': 'Noviembre', '12': 'Diciembre'}
    month_name = month_cols[month]
    for ent in range(1,33):
        subset = data[(data.Clave_Ent == ent) & (data['Año'] == int(year))]
        if pd.isnull(subset[month_name].unique()[0]):
            print('Missing data for clave_ent {0}, month {1}'.format(str(ent).zfill(2), month.zfill(2)))
            return
    return(data)

if __name__ == '__main__':
    # Get Arguments from bash.
    print('Ready to download data')
    parser = argparse.ArgumentParser(description= 'Download data from SENSP: delitos del fuero común')
    parser.add_argument('--data_date', type=str, help='Data date for this pipeline task')
    parser.add_argument('--data_dir', type=str, help='Local directory to store raw data')
    parser.add_argument('--local_ingest_file', type=str, help='Local ingest file')
    args = parser.parse_args()

    _data_date = args.data_date
    _data_dir = args.data_dir
    _local_ingest_file = args.local_ingest_file

    data = source_delitos(data_dir=_data_dir, data_date=_data_date)
    data.to_csv(_local_ingest_file, sep='|', na_rep='', index=False)
    print('Written to file: {}'.format(_local_ingest_file))

