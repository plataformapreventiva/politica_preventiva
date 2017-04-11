#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""Utilidades Banxico

Funciones de Descarga y limpieza de Task Banxico
"""

# INCOMPLETO!!!


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


def ingest_banxico_cajeros():
    wget - q - O - 'http://www.banxico.org.mx/consultas-atm/cajeros.json?l=19.432608,-99.133209&b=&r=100000000000000000000000' | jq - -compact-output - r '.contenido[]' > temp.txt

    import argparse
    import datetime
    import requests

    NOMBRES = {
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

    parser = argparse.ArgumentParser(
        description='Ubicajeros API actualizar db')
    parser.add_argument('-r', '--radio', required=False, help='Radio de busqueda en kms.',
                        default='1000')
    parser.add_argument('-l', '--latlon', required=False, help='Latlon de busqueda',
                        default='19.432608,-99.133209')

    args = parser.parse_args()

    CAJEROS_URL = ('http://www.banxico.org.mx/consultas-atm/cajeros.json?l=' +
                   args.latlon + '&b=&r=' + args.radio)

    CAJEROS_URL = (
        'http://www.banxico.org.mx/consultas-atm/cajeros.json?l=19.432608,-99.133209&b=&r=100000000000000000000000')

    CAJERO_URL = 'http://www.banxico.org.mx/consultas-atm/cajeros/info.json'

    print 'Buscando cajeros en ' + CAJEROS_URL
    cajeros_json = requests.get(CAJEROS_URL).json()['contenido']
    total_cajeros = len(cajeros_json)

    CAJEROS_URL = ('http://www.banxico.org.mx/consultas-atm/cajeros.json?l=' +
                   args.latlon + '&b=&r=' + args.radio)
    CAJERO_URL = 'http://www.banxico.org.mx/consultas-atm/cajeros/info.json'
