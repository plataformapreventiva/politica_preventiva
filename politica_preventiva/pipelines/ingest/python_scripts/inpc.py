#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""Utilidades Cenapred

Funciones de Descarga y limpieza de Task Cenapred
"""

import os
import requests
import numpy as np
import pandas as pd
import json
from requests.auth import HTTPDigestAuth
import datetime
from itertools import product
from bs4 import BeautifulSoup
from ftplib import FTP
import requests
import logging
from os import path, makedirs



def ingest_inpc_ciudad(year='2017', historic=False,
 output=None):
    """
    Returns a Pandas with IPC information from INEGI services

    Args:
        (single_year): Download info from a single year
        (historic_until): IF True, download data from 1969 until year
        (output): CSV where to write the results

    Returns:
        Returns two objects
        - Pandas dataframe data
        - metadata: .

    e.g.
        - data, metadata = get_inpc_ciudad_data()

    """
    if not path.exists('logs'):
        makedirs('logs')
    logging.basicConfig(filename='logs/inpc.log', level=logging.DEBUG)

    data = pd.DataFrame()
    metadata = {}

    #dict of ciudades with state code
    dict_ciudades = {
        "7. Area Metropolitana de la Cd. de Mexico":"09",
        "Acapulco, Gro.":"12",
        "Aguascalientes, Ags.":"01",
        "Campeche, Camp.":"04",
        "Cd. Acuna, Coah.":"05",
        "Cd. Jimenez, Chih.":"08",
        "Cd. Juarez, Chih.":"08",
        "Colima, Col.":"06",
        "Cordoba, Ver.":"30",
        "Cortazar, Gto.":"11",
        "Cuernavaca, Mor.":"17",
        "Culiacan, Sin.":"25",
        "Chetumal, Q.R.":"23",
        "Chihuahua, Chih.":"08",
        "Durango, Dgo.":"10",
        "Fresnillo, Zac.":"32",
        "Guadalajara, Jal.":"14",
        "Hermosillo, Son.":"26",
        "Huatabampo, Son.":"26",
        "Iguala, Gro.":"12",
        "Jacona, Mich.":"16",
        "La Paz, B.C.S.":"03",
        "Leon, Gto.":"11",
        "Matamoros, Tamps.":"28",
        "Merida, Yuc.":"31",
        "Mexicali, B.C.":"02",
        "Monclova, Coah.":"05",
        "Monterrey, N.L.":"19",
        "Morelia, Mich.":"16",
        "Oaxaca, Oax.":"20",
        "Puebla, Pue.":"21",
        "Queretaro, Qro.":"22",
        "San Andres Tuxtla, Ver.":"30",
        "San Luis Potosí, S.L.P.":"24",
        "Tampico, Tamps.":"28",
        "Tapachula, Chis.":"07",
        "Tehuantepec, Oax.":"20",
        "Tepatitlan, Jal.":"14",
        "Tepic, Nay.":"18",  #error downloading data from tepic
        "Tijuana, B.C.":"02",
        "Tlaxcala, Tlax.":"29",
        "Toluca, Edo. de Méx.":"15",
        "Torreon, Coah.":"05",
        "Tulancingo, Hgo.":"13",
        "Veracruz, Ver.":"30",
        "Villahermosa, Tab.":"27"
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
    'cmae_2',
    'scian_1',
    'scian_2',
    'scian_3'
    ]

    if historic:
        year_query = "&_anioI=1969&_anioF={0}".format(year)
    else:
        year_query = "&_anioI={0}&_anioF={0}".format(year)

    for ciudad in dict_ciudades:
        ciudad_encoded = ciudad.replace(" ","%20")
        #.encode("utf-8")
        ciudad_id = dict_ciudades[ciudad]
        base = ("http://www.inegi.org.mx/sistemas/indiceprecios/Exportacion.aspx?INPtipoExporta=CSV"
        "&_formato=CSV")

        #tipo niveles
        tipo = ("&_meta=1&_tipo=Niveles&_info=%C3%8Dndices&_orient=vertical&esquema=0&"
            "t=%C3%8Dndices+de+Precios+al+Consumidor&")

        lugar = "st={0}".format(ciudad_encoded)

        serie = ("&pf=inp&cuadro=0&SeriesConsulta=e%7C240123%2C240124%2C240125%"
        "2C240126%2C240146%2C240160%2C240168%2C240186%2C240189%2C240234"
        "%2C240243%2C240260%2C240273%2C240326%2C240351%2C240407%2C240458"
        "%2C240492%2C240533%2C260211%2C260216%2C260260%2C320804%2C320811%2C320859%2C")

        url = base + year_query + tipo + lugar + serie


        try:
            #download metadata
            metadata[ciudad] = pd.read_csv(url,error_bad_lines=False,nrows=5,usecols=[0],header=None).values
            #download new dataframe
            print('trying to download data from {}'.format(ciudad))

            temp = pd.read_csv(url, error_bad_lines=False,skiprows=14,header=None, names=cols)
            temp['ciudad'] = ciudad

            data = pd.concat([data, temp])
            print("Query succesful for city {}".format(ciudad))

        except:
            print ("Error downloading data for: {}".format(ciudad))
            logging.info("Error downloading data for: year={}, historic={}, city={}".format(year, historic, ciudad))

    if output:
        data.to_csv(output, sep='|')
    else:
        return data, metadata

if __name__ == '__main__':
    import argparse
    parser = argparse.ArgumentParser(description='Download IPC for cities')
    
    parser.add_argument('--year', type=str, default='2017',
        help= 'First month to download, as string format yyyy-m')
    parser.add_argument('--historic', type=bool, default=False,
        help= 'If True, download all data until the specified year')
    parser.add_argument('--output', type=str, default='inpc',
        help = 'Name of outputfile')
    
    args = parser.parse_args()
    
    year = args.year
    historic = args.historic
    output = args.output

    if historic:
        ingest_inpc_ciudad(year=year, historic_until=True, output=output)
    else:
        ingest_inpc_ciudad(year=year, output=output)
