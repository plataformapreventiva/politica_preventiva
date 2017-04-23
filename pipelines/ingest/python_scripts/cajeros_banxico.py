#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""Ingesta Cajeros Banxico
    Funciones de Descarga y limpieza de Task Banxico
    cajeros actualizados de la base http://www.banxico.org.mx/consultas-atm/cajeros.json
    Descarga información del cajero y guarda en ../data/ingest/

ToDo()
Guardar cajeros no encontrados.
¿Solo descargar cajeros que no estén en la base de datos?

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
import argparse
import datetime
import requests


def cajeros_banxico(latlon='19.432608,-99.133209', radio='100000000000000000000000'):

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

    CAJEROS_URL = ('http://www.banxico.org.mx/consultas-atm/cajeros.json?l=' +
                   latlon + '&b=&r=' + radio)

    CAJERO_URL = 'http://www.banxico.org.mx/consultas-atm/cajeros/info.json'

    print('Buscando cajeros en ' + CAJEROS_URL)

    cajeros_json = requests.get(CAJEROS_URL).json()['contenido']

    total_cajeros = []
    cajeros_no_encontrados = []

    print("Buscando Información por Cajero")

    for i, cajero_json in enumerate(cajeros_json):

        try:
            cajero = {}
            cajero['id'] = cajero_json['id']
            cajero['clave_institucion'] = cajero_json['cb']
            cajero['lat'] = cajero_json['l']['lat']
            cajero['lon'] = cajero_json['l']['lng']
            cajero['nombre_institucion'] = dict_cajeros[cajero['clave_institucion']]
            url_cajero = (CAJERO_URL + '?id=' + str(cajero['id']) + '&banco=' +
                          str(cajero['clave_institucion']))
            cajero_json = requests.get(url_cajero).json()['contenido']
            cajero['cp'] = str(cajero_json['cp'])
            cajero['horario'] = cajero_json['hs']
            cajero['direccion'] = cajero_json['d']
            cajero['actualizacion'] = str(datetime.datetime.now())
            total_cajeros.append(cajero)
            print("Buscando cajero número " + str(i) + " de " + str(len(cajeros_json)) + " % " +str(round(i/len(cajeros_json)*100,2)))

        except:
            pass

    print('Cajeros agregados: ' + str(len(total_cajeros)))
    data = pd.DataFrame(total_cajeros)
    data.to_csv("../data/cajeros_banxico.csv")
    return True


if __name__ == '__main__':
    cajeros_banxico()
