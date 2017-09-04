#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""Utilidades Sagarpa

"""

import numpy as np
import pandas as pd
import json
from requests.auth import HTTPDigestAuth
from itertools import product
from bs4 import BeautifulSoup
from ftplib import FTP
from re import findall
from requests import post
from os import path, makedirs
import logging

def ingest_sagarpa_avance_agricola(start_date, end_date=None, 
    cultivo="maiz-grano", output=None):
    """
    Creates csv file with Avance Nacional de Siembra for crop 'cultivo'
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
    if not path.exists('logs'):
        makedirs('logs')
    logging.basicConfig(filename='logs/sagarpa.log', level=logging.DEBUG)

    # Define necessary dictionaries

    dict_moda = {1: "R", 2: "T"}

    dict_ciclo = {1: 'OI', 2: 'PV', 3: 'PE'}

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
                'zanahoria': '32900',
                'agave':'500',
                'aguacate':'600',
                'alfalfa-verde':'1500',
                'cacao':'5400',
                'cafe-cereza':'5500',
                'cana-de-azucar':'6500',
                'copra':'9700',
                'durazno':'12400',
                'esparrago':'13100',
                'frambuesa':'13900', 
                'gerbera':'14900', 
                'guayaba':'16000', 
                'limon':'19000', 
                'maguey-pulquero':'19600', 
                'mango':'20400', 
                'manzana':'20600', 
                'naranja':'22400', 
                'nopalitos':'23000', 
                'nuez':'23200', 
                'papaya':'24700', 
                'pera':'25100',
                'pina':'25800',
                'platano':'26700', 
                'rosa':'28200', 
                'toronja':'31200', 
                'tuna':'31900', 
                'uva':'32000', 
                'zarzamora':'33300'}


    url = "http://infosiap.siap.gob.mx:8080/agricola_siap_gobmx/ResumenProducto.do"
    start_year = int(start_date.split('-')[0])
    start_month = int(start_date.split('-')[1])

    if not end_date:
        end_year = start_year
        end_month = start_month + 1
    else:
        end_year = int(end_date.split('-')[0])
        end_month = int(end_date.split('-')[1])


    perennes = {'agave', 'aguacate', 'alfalfa-verde', 'cacao', 'cafe-cereza', 'cana-de-azucar',
    'copra', 'durazno', 'esparrago', 'frambuesa', 'gerbera', 'guayaba','limon', 'maguey-pulquero', 
    'mango', 'manzana', 'naranja', 'nopalitos', 'nuez','papaya', 'pera', 'pina', 'platano', 'rosa', 
    'toronja', 'tuna', 'uva', 'zarzamora'}

    anios = list(range(start_year, end_year + 1))
    meses = list(range(1, 13))
    moda = list(range(1, 3))
    ciclo = list(range(1, 3))
    if cultivo in perennes:
        ciclo = [3]
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
                response = post(url, params=payload)
            except Exception:
                logging.info('Connection error: year={}, month={}, cicle={}, mode={}'.format(year, 
                    month, dict_ciclo[ciclo], dict_moda[moda]))
                print('Connection error: year={}, month={}, cicle={}, mode={}'.format(year, 
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

                    # Iterate over rows
                    for row in table.findAll('tr'):
                        tds = row.find_all('td')

                        # Table format contains summaries of the data in the middle of the table;
                        # since they are not <td>, we can simply test for their
                        # absence
                        if tds:
                            records.append([' '.join(elem.text.lower().split()) for elem in tds])
            

                    # Add payload information to the table
                    for row in records:
                        row.extend([month, year, dict_moda[moda],
                                    dict_ciclo[ciclo], cultivo.lower()])

                    # Add successful response to the main table
                    results.extend(records)
                else:
                    print(':/       No table found')

    # TODO: check column names and test if colnames have changed
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
        result.to_csv(file_name, sep='|', index=False)
    
    else:
        file = open(file_name,'w')
        file.close()
        file = open('missing.txt','w')
        file.write(file_name)
        file.close()

    return pd.DataFrame(results, columns=col_names)


def ingesta_sagarpa_cierre_produccion(start_date, end_date=None, estado='1',
    output=None):
    """
    Returns a Pandas with Avance Nacional de Siembra for crop 'cultivo'
    from SAGARPA-SIEP. The information is divided by municipality, and
    contains info for hydrolocial mode and cicle of agricultural year.
    This does not take months into account. And note that cierre from 1980-2013 is easier to 
    access from http://www.sagarpa.gob.mx/quienesomos/datosabiertos/siap/Paginas/estadistica.aspx
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
    if not path.exists('logs'):
        makedirs('logs')
    logging.basicConfig(filename='logs/sagarpa-cierre.log', level=logging.DEBUG)


    # Define necessary dictionaries
    dict_moda = {1: "R", 2: "T"}

    dict_ciclo = {1: 'OI', 2: 'PV', 3:'PE'}

    # dict_edos contains the number of municipalities in each state
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

    start_year = int(start_date.split('-')[0])
    #start_month = int(start_date.split('-')[1])

    if not end_date:
        end_year = start_year

    else:
        end_year = int(end_date.split('-')[0])


    anios = list(range(start_year, end_year + 1))
    moda = list(range(1, 3))
    # Nota: dado que en este caso no hay cultivos, no hay por qué separar ciclos 
    ciclo = list(range(1, 4))
    municipios = list(range(1, 700))
    #estados = list(range(1, 3))
    results = []


    # Iterate over years, hydrologic mode and cicle (otonio-invierno
    # or primavera-verano)
    for year, moda, ciclo in product(anios, moda, ciclo):
        for municipio in list(range(1, 1+int(dict_edos[str(estado)]))):

            # Test for dates that are yet to occur
            if (year < end_year+1) and (year >= start_year):
                print('Retrieving year={}, cicle={}, mode={}, estado={},municipality={}'.format(year,
                    dict_ciclo[ciclo], dict_moda[moda], estado, municipio))

                # Create payload to post

                payload = {'pComponente': '', 'pCveCiclo': str(ciclo), 'pAnio': str(year),
                           'pCveEdo': str(estado), 'pCveDDR': '0', 'pCveMpio': str(municipio), 'pCveModalidad': str(moda),
                           'pTpoCultivo': "0", 'pOrden': '0'}

                # Get post response
                try:
                    response = post(url, params=payload)
                except Exception:
                    logging.info('Connection error for year={}, cicle={}, mode={}, state={}, mun={}'.format(year,
                        dict_ciclo[ciclo], dict_moda[moda], estado, municipio))
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
                        edo = findall("Estado ([A-Za-z]*)", edo)
                        mun = soup.find(
                            "div", {"class": "textoTablaSubtitulo2"}).text
                        mun = findall("Municipio: ([A-Za-z.]*.*)", mun)

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
        result = pd.DataFrame(temp)
        result.to_csv(file_name, sep='|')
    
    else:
        file = open(file_name,'w')
        file.close()
        file = open('missing.txt','w')
        file.write(file_name)
        file.close()

    #return pd.DataFrame(results, columns=col_names)

if __name__ == '__main__':
    import argparse
    parser = argparse.ArgumentParser(description='Download SAGARPAs Avance Agrícola \
        and Cierre de Cultivo. Avance Agrícola is downloaded by crop (--cult), Cierre \
        is downloaded by state (--estado) in INEGI codes (1-32).')
    
    parser.add_argument('--start', type=str, default='2004-1',
        help= 'First month to download, as string format yyyy-m')
    parser.add_argument('--end', type=str, default='',
        help= 'Last month to download, as string format yyyy-m. \
        If missing, only --start will be downloaded')
    parser.add_argument('--cult', type=str, default='maiz-grano',
        help = 'Crop to download from SAGARPA, in low case, words separated by hyphen')
    parser.add_argument('--cierre', type=bool, default=False,
        help = 'Option for end-of-season data')
    parser.add_argument('--estado', type=str, default='1', 
        help = 'Estado to download for cierre de cultivo')
    parser.add_argument('--output', type=str, default='',
        help = 'Name of outputfile')
    
    args = parser.parse_args()
    
    start_date = args.start
    end_date = args.end
    cultivo = args.cult
    estado = args.estado
    cierre = args.cierre
    output = args.output

    if cierre:
        ingesta_sagarpa_cierre_produccion(start_date=start_date, end_date=end_date, estado=estado, output=output)
    else:
        ingest_sagarpa_avance_agricola(start_date=start_date, end_date=end_date, cultivo=cultivo, output=output)
