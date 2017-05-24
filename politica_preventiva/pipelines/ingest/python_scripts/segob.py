#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""Utilidades Segob

Funciones de Descarga y limpieza de Task Segob
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


def ingest_segob_snim(output):

    #estados = list(range(1, 33))
    estados = list(range(1,4))
    full_data = []
    dict_edos = {
        "1":  "11",
        "2":  "5",
        "3":  "5",
        "4":  "11",
        "5":  "38",
        "6":  "10",
        "7":  "118",
        "8":  "67",
        "9":  "16",
        "10":  "39",
        "11":  "46",
        "12":  "81",
        "13":  "84",
        "14":  "125",
        "15":  "125",
        "16":  "113",
        "17":  "33",
        "18":  "20",
        "19":  "51",
        "20":  "570",
        "21":  "217",
        "22":  "18",
        "23":  "9",
        "24":  "58",
        "25":  "18",
        "26":  "72",
        "27":  "17",
        "28":  "43",
        "29":  "60",
        "30":  "212",
        "31":  "106",
        "32":  "58"}
    
    for estado in estados:
        for municipio in list(range(1, 1+int(dict_edos[str(estado)]))):
            cookies = {
                'jquery-ui-theme': 'Smoothness',
            }

            headers = {
                'Origin': 'http://www.snim.rami.gob.mx',
                'Accept-Encoding': 'gzip, deflate',
                'Accept-Language': 'es-MX,es;q=0.8,es-419;q=0.6,en;q=0.4',
                'User-Agent': 'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/54.0.2840.59 Safari/537.36',
                'Content-Type': 'application/x-www-form-urlencoded',
                'Accept': '*/*',
                'Referer': 'http://www.snim.rami.gob.mx/',
                'X-Requested-With': 'XMLHttpRequest',
                'Connection': 'keep-alive',
            }

            data = {
                'edo': '{0}'.format(str(estado)),
                'mun': '{0}'.format(str(municipio)),
                'tipo': 'm',
                'reporte': 'dg2010'
            }

            try:
                response = requests.post(
                    'http://www.snim.rami.gob.mx/tbl_poblacion.php', headers=headers, cookies=cookies, data=data)
            except:
                pass

            # parse text
            soup = BeautifulSoup(response.text, 'html.parser').find('tbody')
            headers = [h.text.encode('utf-8') for h in soup.find_all("th")]
            values = [h.text.encode('utf-8') for h in soup.find_all("td")]
            json_data = {}

            json_data["cve_muni"] = str(estado).zfill(
                2) + str(municipio).zfill(3)
            for header, value in zip(headers, values):
                json_data[header] = value
            full_data.append(json_data)

            print("getting data from municipality : " +
                  str(estado).zfill(2) + str(municipio).zfill(3))

    file = pd.DataFrame(full_data)
    file.to_csv(output, sep='|')

if __name__ == '__main__':
    import argparse
    parser = argparse.ArgumentParser(description='Download SEGOB data for municipalities')

    parser.add_argument('--output', type=str, default='segob',
        help = 'Name of output file')

    args = parser.parse_args()
    output = args.output

    ingest_segob_snim(output)

