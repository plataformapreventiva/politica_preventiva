#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
Ingesta Cajeros Banxico
    Función de Ingesta de Task Banxico
    cajeros actualizados de la base http://www.banxico.org.mx/consultas-atm/cajeros.json
"""

import os
import json
import sys
import datetime
import requests
import argparse
import datetime
import numpy as np
import pandas as pd
from requests.auth import HTTPDigestAuth
from itertools import product
from bs4 import BeautifulSoup
from os import path, makedirs
from ftplib import FTP
import logging

module_parent = '../../'
script_dir = os.path.dirname(os.path.realpath(os.path.join(os.getcwd(), os.path.expanduser(__file__))))
sys.path.append(os.path.normpath(os.path.join(script_dir, module_parent)))
from utils.postgres_utils import connect_to_db

def cajeros_banxico(output, latlon='19.432608,-99.133209',
                    radio='100000000000000000000000',solo_nuevos=False):

    """
    Función que descarga de la página de www.banxico.org.mx/consultas-atm/cajeros.json
    todos los cajeros que existen en ese momento.

    Args:
        latlon (str): String with latitud and longitud. The default value is
                      Mexico centroid.
        radio (str): Search radius in meters.
        solo_nuevos (booleano): Only stores new ids

    Returns:
        True if task is completed
            It also saves a DataFrame with all Bank cashier information from Banxico.
            in output as csv with sep='|'.

    """
    #logging.info('Iniciando Descarga de pipeline_task cajeros_banxico')

    # Create logging
    if not path.exists('logs'):
        makedirs('logs')
    #logging.basicConfig(filename='logs/cajeros_banxico.log',
    #                    level=logging.DEBUG)

    # cajeros ids
    dict_cajeros = {
        40138: 'ABC CAPITAL',
        40062: 'AFIRME',
        40128: 'AUTOFIN',
        40127: 'AZTECA',
        40030: 'BAJIO',
        40002: 'BANAMEX',
        40131: 'BANCO FAMSA',
        40137: 'BANCOPPEL',
        37019: 'BANJERCITO',
        40072: 'BANORTE-IXE',
        40058: 'BANREGIO',
        37166: 'BANSEFI',
        40060: 'BANSI',
        40012: 'BBVA BANCOMER',
        40132: 'BMULTIVA',
        40143: 'CIBANCO',
        40021: 'HSBC',
        40036: 'INBURSA',
        40037: 'INTERACCIONES',
        40042: 'MIFEL',
        40014: 'SANTANDER',
        40044: 'SCOTIABANK',
        40134: '',
        40136: '',
        40147: '',
        1: 'WAL-MART',
        2: 'MERCO',
        3: 'CHEDRAUI',
        4: 'BODEGA AURRERA',
        5: 'SAMS',
        6: 'SUBURBIA',
        7: 'SUPERAMA'
    }

    # First API call to get all cajeros ids
    CAJEROS_URL = ('http://www.banxico.org.mx/consultas-atm/cajeros.json?l=' +
                   latlon + '&b=&r=' + radio)
    print('Buscando cajeros en ' + CAJEROS_URL)
    cajeros_json = requests.get(CAJEROS_URL).json()['contenido']

    # If only new ids want to be stored
    total_cajeros = []
    cajeros_no_encontrados = []
    if solo_nuevos == True:
        # check database
        logging.info('Checando si existen cajeros nuevos para pipeline_task: {}'.
                     format(len(cajeros_json)))
        conn = connect_to_db()
        cur = conn.cursor()
        cur.execute("""SELECT  id FROM raw.cajeros_banxico""")
        ans = cur.fetchall()
        ids_old = []

        for row in ans:
            ids_old.append(row[0])

        ids_new = [d['id'] for d in cajeros_json if 'id' in d]
        ids = set(ids_new) - set(ids_old)
        cajeros_json = [x for x in cajeros_json if x.get(
            'id', None) in set(ids) and x.get('id', None) is not None]

    else:
        pass

    logging.info('Identificando cajeros nuevos para pipeline_task: {} '.format(
        len(cajeros_json)))

    cajeros_json
    # Second API call to get all info for each cajero id
    CAJERO_URL = 'http://www.banxico.org.mx/consultas-atm/cajeros/info.json'
    for i, cajero_json in enumerate(cajeros_json):
       try:
           cajero = {}
           cajero['id'] = cajero_json['id']
           cajero['clave_institucion'] = cajero_json['cb']
           cajero['lat'] = cajero_json['l']['lat']
           cajero['lon'] = cajero_json['l']['lng']
           cajero['nombre_institucion'] = dict_cajeros[
               cajero['clave_institucion']]
           url_cajero = (CAJERO_URL + '?id=' + str(cajero['id']) + '&banco=' +
                         str(cajero['clave_institucion']))
           cajero_json = requests.get(url_cajero).json()['contenido']
           cajero['cp'] = str(cajero_json['cp'])
           cajero['horario'] = cajero_json['hs']
           cajero['direccion'] = cajero_json['d']
           cajero['actualizacion'] = str(datetime.datetime.now())
           total_cajeros.append(cajero)
           print("Buscando cajero número " + str(i) + " de " + str(len(cajeros_json)) +
                 " % " + str(round(i/len(cajeros_json)*100, 2)))

       except:
           pass

    logging.info('Cajeros agregados:{}'.format(str(len(total_cajeros))))
    # Store info
    data = pd.DataFrame(total_cajeros)
    data.replace(to_replace='|', value='-', inplace=True)
    data.to_csv(output, sep='|', encoding="utf-8", index=False)

    return True


if __name__ == '__main__':

    # Get Arguments from bash.
    parser = argparse.ArgumentParser(description= 'Download Cajeros Banxico')
    parser.add_argument('--output', type=str, default='',
                        help = 'Name of outputfile')
    # Read arguments
    args = parser.parse_args()
    output = args.output
    # Start function.
    cajeros_banxico(output=output,
                    solo_nuevos=False)

