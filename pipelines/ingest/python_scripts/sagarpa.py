#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""Utilidades Sagarpa

Funciones de Descarga y limpieza de Task Sagarpa
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
import re

def ingest_sagarpa_avance_agricola(start_date, end_date=None, 
    cultivo="maiz-grano", output=None):
    """
    Returns a Pandas with Avance Nacional de Siembra for crop 'cultivo'
    from SAGARPA-SIEP. The information is divided by municipality, and
    contains info for hydrolocial mode and cicle of agricultural year.

    Args:
        (start_date): format 'year-m'
        (end_date) : format 'year-m'. If None, only start_date is downloaded
        (cultivo): Crop to monitor. List available from dict_cultivo-

    Returns:
        Pandas dataframe with columns:
            (estado):
            (distrito): division above municipality for agro-purposes
            (municipio):
            (sup_sembrada): sowed land (hc)
            (sup_cosech): harvested land (hc)
            (sup_siniest): lost land (hc)
            (prod): produce (tons)
            (rendim): yield (tons/hc)
            (mes):
            (anio):
            (moda_hidr): hydrological mode
                R: irrigated (riego)
                T: rainfall (temporal)
            (ciclo): cicle of agricultural year
                OI: Fall-Winter
                PV: Spring-Summer
            (cultivo): crop to monitor (same as arg)
    """
    # Define necessary dictionaries
    dict_moda = {1: "R", 2: "T"}

    dict_ciclo = {1: 'OI', 2: 'PV'}

    dict_cultivo = {'ajo': '700',
                    'ajonjoli': '800',
                    'algodon-hueso': '1800',
                    'amaranto': '2800',
                    'arroz-palay': '3300',
                    'avena-forrajera-en-verde': '3900',
                    'avena-grano': '4000',
                    'berenjena': '4600',
                    'brocoli': '5100',
                    'calabacita': '5800',
                    'cartamo': '6900',
                    'cebada-grano': '7300',
                    'cebolla': '7400',
                    'chile-verde': '11400',
                    'coliflor': '9000', 
                    'crisantemo': '10130',
                    'elote': '12700',
                    'fresa': '14000',
                    'frijol': '14200',
                    'garbanzo': '14700',
                    'gladiola': '15400',
                    'lechuga': '18500',
                    'maiz-forrajero-en-verde': '19800',
                    'maiz-grano': '19700',
                    'melon': '21200',
                    'papa': '24400',
                    'pepino': '24900',
                    'sandia': '28700',
                    'sorgo-forrajero-en-verde': '29300',
                    'sorgo-grano': '29500',
                    'soya': '29700',
                    'tabaco': '30000',
                    'tomate-rojo': '30800',
                    'tomate-verde': '31000',
                    'trigo-grano': '31500',
                    'zanahoria': '32900'}


    url = "http://infosiap.siap.gob.mx:8080/agricola_siap_gobmx/ResumenProducto.do"
    start_year = int(start_date.split('-')[0])
    start_month = int(start_date.split('-')[1])

    if not end_date:
        end_year = start_year
        end_month = start_month + 1
    else:
        end_year = int(end_date.split('-')[0])
        end_month = int(end_date.split('-')[1])


    anios = list(range(start_year, end_year + 1))
    meses = list(range(1, 13))
    moda = list(range(1, 3))
    ciclo = list(range(1, 3))
    results = []

    # Iterate over years, months, hidrologyc mode and cicle (otonio-invierno
    # or primavera-verano)
    for year, month, moda, ciclo in product(anios, meses, moda, ciclo):

        # Test for dates that are yet to occur
        if (month < end_month or year < end_year) and (month >= start_month or year > start_year):
            print('Retrieving year={}, month={}, cicle={}, mode={}'.format(year,
                                                                           month, dict_ciclo[ciclo], dict_moda[moda]))

            # Create payload to post
            payload = {'anio': str(year), 'nivel': '3', 'delegacion': '0', 'municipio': '-1',
                       'mes': str(month), 'moda': str(moda), 'ciclo': str(ciclo),
                       'producto': dict_cultivo[cultivo], 'tipoprograma': '0',
                       'consultar': 'si', 'invitado': 'false'}

            # Get post response
            try:
                response = requests.post(url, params=payload)
            except Exception:
                print('##### Connection error for year={}, month={}, cicle={}, mode={}'.format(year,
                                                                                               month, dict_ciclo[ciclo], dict_moda[moda]))
                response = False

            # Test for response
            if response:
                print('Successful response!')

                # Get information table from HTLM response
                soup = BeautifulSoup(response.text, 'html.parser')
                table = soup.find('table', attrs={
                                  'class': 'table table-responsive table-striped table-bordered'})

                # Iterate over table rows and extract information. Since the response lacks 'estado' for
                # a state's second and subsequent occurances, we add 'estado' with the
                # help of a boolean variable 'keep' and  a response variable
                # 'keep_state'
                if table:
                    print(':D       Table found')
                    records = []
                    keep = True

                    # Iterate over rows
                    for row in table.findAll('tr'):
                        tds = row.find_all('td')

                        # Table format contains summaries of the data in the middle of the table;
                        # since they are not <td>, we can simply test for their
                        # absence
                        if tds:
                            test = "".join(tds[0].text.split())
                            if keep and test:
                                keep_state = tds[0]
                                keep = False
                            tds[0] = keep_state
                            records.append(
                                [' '.join(elem.text.lower().split()) for elem in tds])
                        else:
                            keep = True

                    # Add payload information to the table
                    for row in records:
                        row.extend([month, year, dict_moda[moda],
                                    dict_ciclo[ciclo], cultivo.lower()])

                    # Add successful response to the main table
                    results.extend(records)
                else:
                    print(':/       No table found')

    col_names = ['estado', 'distrito', 'municipio', 'sup_sembrada', 'sup_cosech',
                 'sup_siniest', 'produccion', 'rendim', 'mes', 'anio', 'moda_hidr', 'ciclo', 'cultivo']
    
    # Write file trto csv
    if output:
        file_name = output
    else:
        dates = end_date + '--sagarpa--' + start_date + '--'
        file_name = '../data/sagarpa/' + dates + cultivo
    
    if results:
        result = pd.DataFrame(results, columns=col_names)
        result.to_csv(file_name +'.csv')
    
    else:
        file = open(file_name + '.csv','w')
        file.close()
        file = open('missing.txt','w')
        file.write(file_name)
        file.close()

    return pd.DataFrame(results, columns=col_names)


def ingesta_sagarpa_cierre_produccion(start_date, end_date=None, 
    cultivo="maiz-grano", output=None):
    """
    Returns a Pandas with Avance Nacional de Siembra for crop 'cultivo'
    from SAGARPA-SIEP. The information is divided by municipality, and
    contains info for hydrolocial mode and cicle of agricultural year.
    This does not take months into account 
    Args:
        (start_date): format 'year-m'
        (end_date) : format 'year-m'. If None, only start_date is downloaded
        (cultivo): Crop to monitor. List available from dict_cultivo-

    Returns:
        Pandas dataframe with columns:
            (estado):
            (distrito): division above municipality for agro-purposes
            (municipio):
            (sup_sembrada): sowed land (hc)
            (sup_cosech): harvested land (hc)
            (sup_siniest): lost land (hc)
            (prod): produce (tons)
            (rendim): yield (tons/hc)
            (mes):
            (anio):
            (moda_hidr): hydrological mode
                R: irrigated (riego)
                T: rainfall (temporal)
            (ciclo): cicle of agricultural year
                OI: Fall-Winter
                PV: Spring-Summer
            (cultivo): crop to monitor (same as arg)
    """
    # Define necessary dictionaries
    dict_moda = {1: "R", 2: "T"}

    dict_ciclo = {1: 'OI', 2: 'PV'}

    dict_cultivo = {'ajo': '700',
                    'ajonjoli': '800',
                    'algodon-hueso': '1800',
                    'amaranto': '2800',
                    'arroz-palay': '3300',
                    'avena-forrajera-en-verde': '3900',
                    'avena-grano': '4000',
                    'berenjena': '4600',
                    'brocoli': '5100',
                    'calabacita': '5800',
                    'cartamo': '6900',
                    'cebada-grano': '7300',
                    'cebolla': '7400',
                    'chile-verde': '11400',
                    'coliflor': '9000', 
                    'crisantemo': '10130',
                    'elote': '12700',
                    'fresa': '14000',
                    'frijol': '14200',
                    'garbanzo': '14700',
                    'gladiola': '15400',
                    'lechuga': '18500',
                    'maiz-forrajero-en-verde': '19800',
                    'maiz-grano': '19700',
                    'melon': '21200',
                    'papa': '24400',
                    'pepino': '24900',
                    'sandia': '28700',
                    'sorgo-forrajero-en-verde': '29300',
                    'sorgo-grano': '29500',
                    'soya': '29700',
                    'tabaco': '30000',
                    'tomate-rojo': '30800',
                    'tomate-verde': '31000',
                    'trigo-grano': '31500',
                    'zanahoria': '32900'}
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

    url = "http://infosiap.siap.gob.mx/aagricola_siap_gb/ientidad/index.jsp"
    now = datetime.datetime.now()

    start_year = int(start_date.split('-')[0])
    start_month = int(start_date.split('-')[1])

    if not end_date:
        end_year = start_year
        end_month = start_month + 1
    else:
        end_year = int(end_date.split('-')[0])
        end_month = int(end_date.split('-')[1])

    anios = list(range(start_year, end_year + 1))
    moda = list(range(1, 3))
    ciclo = list(range(1, 3))
    municipios = list(range(1, 700))
    estados = list(range(1, 33))

    results = []

    # Iterate over years, hydrologic mode and cicle (otonio-invierno
    # or primavera-verano)
    for year, moda, ciclo, estado in product(anios, moda, ciclo, estados):
        for municipio in list(range(1, int(dict_edos[str(estado)]))):

            # Test for dates that are yet to occur
            if (year < end_year+1) and (year >= start_year):
                print('Retrieving year={}, cicle={}, mode={},estado={},municipality={}'.format(year,
                    dict_ciclo[ciclo], dict_moda[moda], estado, municipio))

                # Create payload to post

                payload = {'pComponente': '', 'pCveCiclo': str(ciclo), 'pAnio': str(year),
                           'pCveEdo': str(estado), 'pCveDDR': '0', 'pCveMpio': str(municipio), 'pCveModalidad': str(moda),
                           'pTpoCultivo': "0", 'pOrden': '0'}

                # Get post response
                try:
                    response = requests.post(url, params=payload)
                except Exception:
                    print('##### Connection error for year={}, cicle={}, mode={}, state={}, mun={}'.format(year,
                        dict_ciclo[ciclo], dict_moda[moda], estado, municipio))
                    response = False

                # Test for response
                if response:
                    print('Successful response!')

                    # Get information table from HTLM response
                    soup = BeautifulSoup(response.text, 'html.parser')
                    #tree = html.fromstring(response.content)
                    table = soup.find(
                        'table', attrs={'class': 'table table-striped table-bordered'})

                    # help of a boolean variable 'keep' and  a response
                    # variable 'keep_state'
                    if table:
                        print(':D       Table found')
                        records = []
                        keep = True

                        edo = soup.find(
                            "div", {"class": "textoTablaTitulo"}).text
                        edo = re.findall("Estado ([A-Za-z]*)", edo)
                        mun = soup.find(
                            "div", {"class": "textoTablaSubtitulo2"}).text
                        mun = re.findall("Municipio: ([A-Za-z.]*.*)", mun)

                        # Iterate over rows
                        for row in table.findAll('tr'):
                            tds = row.findAll('td')

                            # Table format contains summaries of the data in the middle of the table;
                            # since they are not <td>, we can simply test for
                            # their absence
                            if tds:
                                test = "".join(tds[0].text.split())
                                if keep and test:
                                    keep_state = tds[0]
                                    keep = False
                                tds[0] = keep_state
                                records.append(
                                    [' '.join(elem.text.lower().split()) for elem in tds])
                            else:
                                keep = True

                        # Add payload information to the table
                        for row in records:
                            row.extend([year, dict_moda[moda],
                                        dict_ciclo[ciclo], edo[0], mun[0]])

                        # Add successful response to the main table
                        results.extend(records)
                    else:
                        print(':/       No table found')

    col_names = ['n', 'A', 'B', 'cultivo', 'variedad', 'sup_sembrada_ha', 'sup_cosech_ha',
                 'Sup_Siniestrada', 'Producción_Ton', 'Rendimiento_Ton_Ha', 'PMR_$_Ton', 'valor_produccion_k',
                 'anio', 'moda_hidr', 'ciclo', 'estado', 'municipio']
    temp = pd.DataFrame(results, columns=col_names)
    temp.drop('B', 1, inplace=True)
    temp.drop('A', 1, inplace=True)

    if output:
        file_name=output
    else:
        dates = end_date + '--sagarpa_cierre--' + start_date + '--'
        file_name = '../data/sagarpa_cierre/' + dates + cultivo
    
    if results:
        result = pd.DataFrame(temp, columns=col_names)
        result.to_csv(file_name +'.csv')
    
    else:
        file = open(file_name + '.txt','w')
        file.close()

    return pd.DataFrame(results, columns=col_names)

if __name__ == '__main__':
    import argparse
    parser = argparse.ArgumentParser(description='Dowload SAGARPAs Avance Agricola')
    
    parser.add_argument('--start', type=str, default='2004-1',
        help= 'First month to download, as string format yyyy-m')
    parser.add_argument('--end', type=str, default='',
        help= 'Last month to download, as string format yyyy-m. If')
    parser.add_argument('--cult', type=str, default='maiz-grano',
        help = 'Crop to download from SAGARPA, in low case, words separated by hyphen')
    parser.add_argument('--cierre', type=bool, default=False,
        help = 'Option for end-of-season data')
    parser.add_argument('--output', type=bool, default=False,
        help = 'Name of outputfile')
    
    args = parser.parse_args()
    
    start_date = args.start
    end_date = args.end
    cultivo = args.cult
    cierre = args.cierre
    output = args.output
    if cierre:
        ingesta_sagarpa_cierre_produccion(start_date, end_date, cultivo)
    else:
        ingest_sagarpa_avance_agricola(start_date, end_date, cultivo)
