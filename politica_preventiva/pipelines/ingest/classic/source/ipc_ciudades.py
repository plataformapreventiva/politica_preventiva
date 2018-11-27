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
import numpy as np
import pandas as pd

from os import path, makedirs


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
    if not path.exists('logs'):
        makedirs('logs')
    logging.basicConfig(filename='logs/inpc.log', level=logging.DEBUG)

    data = pd.DataFrame()
    metadata = {}

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

    # dict of ciudades with state code
    dict_ciudades = {
        "7. Area Metropolitana de la Cd. de Mexico": "09",
        "Acapulco, Gro.": "12",
        "Aguascalientes, Ags.": "01",
        "Atlacomulco, Mex.": "15",
        "Campeche, Camp.": "04",
        "Cancun, Q.R.": "23",
        "Cd. Acuna, Coah.": "05",
        "Cd. Jimenez, Chih.": "08",
        "Cd. Juarez, Chih.": "08",
        "Coatzacoalcos, Ver.": "30",
        "Colima, Col.": "06",
        "Cordoba, Ver.": "30",
        "Cortazar, Gto.": "11",
        "Cuernavaca, Mor.": "17",
        "Culiacan, Sin.": "25",
        "Chetumal, Q.R.": "23",
        "Chihuahua, Chih.": "08",
        "Durango, Dgo.": "10",
        "Esperanza, Son.": "26",
        "Fresnillo, Zac.": "32",
        "Guadalajara, Jal.": "14",
        "Hermosillo, Son.": "26",
        "Huatabampo, Son.": "26",
        "Iguala, Gro.": "12",
        "Izucar de Matamoros, Pue.": "21",
        "Jacona, Mich.": "16",
        "La Paz, B.C.S.": "03",
        "Leon, Gto.": "11",
        "Matamoros, Tamps.": "28",
        "Merida, Yuc.": "31",
        "Mexicali, B.C.": "02",
        "Monclova, Coah.": "05",
        "Monterrey, N.L.": "19",
        "Morelia, Mich.": "16",
        "Oaxaca, Oax.": "20",
        "Pachuca, Hgo.": "13",
        "Puebla, Pue.": "21",
        "Queretaro, Qro.": "22",
        "Saltillo, Coah": "05",
        "San Andres Tuxtla, Ver.": "30",
        "San Luis Potosi, S.L.P.": "24",
        "Tampico, Tamps.": "28",
        "Tapachula, Chis.": "07",
        "Tehuantepec, Oax.": "20",
        "Tepatitlan, Jal.": "14",
        "Tepic, Nay.": "18",
        "Tijuana, B.C.": "02",
        "Tlaxcala, Tlax.": "29",
        "Toluca, Mex.": "15",
        "Torreon, Coah.": "05",
        "Tulancingo, Hgo.": "13",
        "Tuxtla Gutierrez, Chis.": "07",
        "Veracruz, Ver.": "30",
        "Villahermosa, Tab.": "27"
    }

    cols = [
        'fecha',
        'indice',
        'alim_bt',
        'alim',
        'alim_pantc',
        'alim_car',
        'alim_pescm',
        'alim_lech',
        'alim_aceig',
        'alim_fruth',
        'alim_azucf',
        'alim_otr',
        'alim_alcht',
        'ropa',
        'viv',
        'mueb',
        'salu',
        'transp',
        'edu',
        'otro',
        'cmae_1',
        'cmae_2',
        'cmae_3',
        'scian_1',
        'scian_2',
        'scian_3'
    ]

    if historic:
        year_query = "&_anioI=1969&_anioF={0}".format(year)
    else:
        year_query = "&_anioI={0}&_anioF={0}".format(year)

    for ciudad in dict_ciudades:
        ciudad_encoded = ciudad.replace(" ", "%20")
        ciudad_id = dict_ciudades[ciudad]
        base = ("http://www.inegi.org.mx/sistemas/indiceprecios/Exportacion.aspx?INPtipoExporta=CSV"
                "&_formato=CSV")

        # tipo niveles
        tipo = ("&_meta=1&_tipo=Niveles&_info=%C3%8Dndices&_orient=vertical&esquema=0&"
                "t=%C3%8Dndices+de+Precios+al+Consumidor&")

        lugar = "st={0}".format(ciudad_encoded)

        serie = ("&pf=inp&cuadro=0&SeriesConsulta=e%7C240123%2C240124%2C240125%"
                 "2C240126%2C240146%2C240160%2C240168%2C240186%2C240189%2C240234"
                 "%2C240243%2C240260%2C240273%2C240326%2C240351%2C240407%2C240458"
                 "%2C240492%2C240533%2C260211%2C260216%2C260260%2C320804%2C320811%2C320859%2C")

        url = base + year_query + tipo + lugar + serie

        try:
            # download metadata
            metadata[ciudad] = pd.read_csv(url, error_bad_lines=False,
                                           nrows=5, usecols=[0],
                                           header=None, encoding='latin-1').values
            # download new dataframe
            print('trying to download data from {}'.format(ciudad))

            temp = pd.read_csv(url, error_bad_lines=False,
                               skiprows=14, header=None, names=cols,
                               encoding='latin-1').\
                   query('fecha == "{}"'.format(period))
            temp['ciudad'] = ciudad

            data = pd.concat([data, temp])
            print("Query succesful for city {}".format(ciudad))

        except:
            print("Error downloading data for: {}".format(ciudad))
            logging.info("Error downloading data for: year={}, historic={}, city={}".format(
                year, historic, ciudad))

    if data.empty:
        return

    if output:
        data.to_csv(output, sep='|', index=False)
    else:
        return data, metadata

if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Download IPC data for cities')

    parser.add_argument('--data_date', type=str,
                        help='Data datefor this pipeline task')
    parser.add_argument('--data_dir', type=str,
                        help='Local directory to store raw data')
    parser.add_argument('--local_ingest_file', type=str,
                        help='Path to local ingest file')
    parser.add_argument('--historic', type=bool, default=False,
                        help='If True, script will download data from first year available until the period specified by the data date parameter')

    args = parser.parse_args()

    _data_date = args.data_date
    _data_dir = args.data_dir
    _local_ingest_file = args.local_ingest_file
    _historic = args.historic

    if _historic:
        data, metadata = ingest_ipc_ciudad(data_date=_data_date, historic=True)
    else:
        data, metadata = ingest_ipc_ciudad(data_date=_data_date)


    print('Downloading data')

    try:
        data.to_csv(_local_ingest_file, sep='|', index=False)
        print('Written to: {}'.format(_local_ingest_file))
    except Exception as e:
        print('Failed to write data to local ingest file')
