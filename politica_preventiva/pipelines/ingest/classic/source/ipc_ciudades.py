#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
Ingesta de los datos del INPC
Fuente: INEGI

Este script ingesta de los datos de los índices que componen el INPC de INEGI,
a nivel ciudad, para las 55 ciudades que componen el INPC.

Se toman como argumentos de entrada, en ese orden: el data date, el directorio
de ingesta local y el path completo del archivo de ingesta local, en formato
csv, separado por pipes "|". La función está pensada para soportar ingestas
históricas si se considera necesario.
"""

import argparse
import datetime
import logging
import pdb

import numpy as np
from os import path, makedirs
import pandas as pd

def ingest_ipc_ciudad(data_date, historic=False, output=None):
    """
    Returns a Pandas with IPC information from INEGI services

    Args:
        (data_date): Period to download data from
        (historic): IF True, download data from 1969 until year
        (output): CSV where to write the results.

    Returns:
        Returns two objects
        - Pandas dataframe data
        - metadata: .

    e.g.
        - data, metadata = get_ipc_ciudad()

    """

    # Extract period in order to filter for month
    year, month = data_date.split('-')
    period = datetime.datetime(int(year), int(month), 1).\
             strftime('%b %Y').\
             title()
    # Ugly hack because we don't have spanish locales in our docker image
    dict_months = {
        "Jan": "Ene",
        "Apr": "Abr",
        "Aug": "Ago",
        "Dec": "Dic"
    }
    for em, sm in dict_months.items():
        period = period.replace(em, sm)

    # Create URL
    if historic:
        year_query = "&_anioI=1969&_anioF={0}".format(year)
    else:
        year_query = "&_anioI={0}&_anioF={0}".format(year)

    base = ("http://www.inegi.org.mx/sistemas/indiceprecios/Exportacion.aspx?INPtipoExporta=CSV"
            "&_formato=CSV")

    tipo = ("&_meta=1&_tipo=Niveles&_info=%C3%8Dndices&_orient=vertical&esquema=0&"
            "t=%C3%8Dndices+de+Precios+al+Consumidor&")

    lugar = "st={0}".format("%C3%8Dndice+Nacional+de+Precios+al+"+\
            "Consumidor%2C+ciudades+que+lo+componen+por+orden+alfab%C3%A9tico+%C2%A0&")

    serie = ("&pf=inp&cuadro=0&SeriesConsulta=e%2C240124%2C240125"
    "%2C240126%2C240146%2C240160%2C240168%2C240186%2C240189%"
    "2C240234%2C240243%2C240260%2C240273%2C240326%2C240351%"
    "2C240407%2C240458%2C240492%2C240533%2C240544%2C240965%"
    "2C241386%2C241807%2C242228%2C242649%2C243070%2C243491%"
    "2C243912%2C244333%2C244754%2C245175%2C245596%2C246017%"
    "2C246438%2C246859%2C247280%2C247701%2C248122%2C248543%"
    "2C248964%2C249385%2C249806%2C250227%2C250648%2C251069%"
    "2C251490%2C251911%2C252332%2C252753%2C253174%2C253595%"
    "2C254016%2C254437%2C254858%2C255279%2C255700%2C256121%"
    "2C256542%2C256963%2C257384%2C257805%2C258226%2C258647%"
    "2C259068%2C260211%2C260216%2C260260%2C260410%2C320804%"
    "2C320811%2C320859%7C240123")
    url = base + year_query + tipo + lugar + serie

    # Get Data
    temp = pd.read_csv(url, error_bad_lines=False, skiprows=13, encoding='latin-1').\
           query('Fecha == "{}"'.format(period))

    # Gather dataframe
    columnas = list(temp.columns)[1:]
    output =  pd.melt(temp, id_vars=['Fecha'], value_vars=columnas)

    # Get metadata
    metadata = pd.read_csv(url, error_bad_lines=False,
            skiprows=5,nrows=8, encoding='latin-1').T
    metadata = metadata.rename(columns=metadata.iloc[0]).\
            drop(metadata.index[0]).reset_index().\
            rename(columns = {"Fecha":"variable", "index":"descripcion"})
    metadata["descripcion"] = metadata["descripcion"].\
            apply(lambda x: x.encode('latin-1', errors="ignore").\
            decode( encoding="utf-8"))
    metadata["Base"] = metadata["Base"].apply(lambda x: x.encode('latin-1',
        errors="ignore").decode( encoding="utf-8"))
    metadata = metadata[["variable", "descripcion", "Base"]]
    metadata["nom_cd"] = metadata['descripcion'].\
            str.extract(r'.*Por ciudad,(.*).Por.*')

    if output.empty:
        return

    data = output.merge(metadata)

    return data

if __name__ == '__main__':

    parser = argparse.ArgumentParser(description='Download IPC data for cities')

    parser.add_argument('--data_date', type=str,
                        help='Data datefor this pipeline task')
    parser.add_argument('--data_dir', type=str,
                        help='Local directory to store raw data')
    parser.add_argument('--local_ingest_file', type=str,
                        help='Path to local ingest file')
    parser.add_argument('--historic', type=bool, default=False,
                        help='If True, script will download data from first '+\
                                'year available until the period specified '+\
                                'by the data date parameter')

    args = parser.parse_args()

    _data_date = args.data_date
    _data_dir = args.data_dir
    _local_ingest_file = args.local_ingest_file
    _historic = args.historic

    if _historic:
        data = ingest_ipc_ciudad(data_date=_data_date, historic=True)
    else:
        data = ingest_ipc_ciudad(data_date=_data_date)

    print('Downloading data')

    try:
        data.to_csv(_local_ingest_file, sep='|', index=False)
        print('Written to: {}'.format(_local_ingest_file))

    except Exception as e:

        print('Failed to write data to local ingest file')
