#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
    Utilidades Secretaría de Economía - SNIIM

"""

import numpy as np
import pandas as pd
import json
import requests
from requests.auth import HTTPDigestAuth
from itertools import product
from bs4 import BeautifulSoup
from ftplib import FTP
from re import findall
from os import path, makedirs
import logging

def ingest_precios(start_date, end_date='', output=''):
    """
    Creates a CSV file with weekly prices from centrales de abasto. 

    Args:
        (start_date): format po'year-m'
        (end_date) : format 'year-m'. If None, only start_date is downloaded
        (output): file name for the CSV 

    Returns:
        None. Writes a CSV file.
    """
    if not path.exists('logs'):
        makedirs('logs')
    logging.basicConfig(filename='logs/precios.log', level=logging.DEBUG)

    #Note: we scrape by central because 'Todos' option doesn't always yield all of the centrales
    centrales_dict = {
        '11' : 'Aguascalientes: Central de Abasto de Aguascalientes',
        '10' : 'Aguascalientes: Centro Comercial Agropecuario de Aguascalientes',
        '33' : 'Baja California : Central de Abasto INDIA, Tijuana',
        '20' : 'Baja California Sur: Unión de Comerciantes de La Paz',
        '40' : 'Campeche: Mercado "Pedro Sáinz de Baranda", Campeche',
        '50' : 'Coahuila: Central de Abasto de La Laguna, Torreón',
        '80' : 'Colima: Centros de distribución de Colima',
        '70' : 'Chiapas: Central de Abasto de Tuxtla Gutiérrez',
        '61' : 'Chihuahua: Central de Abasto de Chihuahua',
        '100' : 'DF: Central de Abasto de Iztapalapa DF',
        '102' : 'Durango: Central de Abasto "Francisco Villa"',
        '101' : 'Durango: Centro de Distribución y Abasto de Gómez Palacio',
        '110' : 'Guanajuato: Central de Abasto de León',
        '112' : 'Guanajuato: Mercado de Abasto de Celaya ("Benito Juárez")',
        '111' : 'Guanajuato: Módulo de Abasto Irapuato',
        '121' : 'Guerrero: Central de Abastos de Acapulco',
        '130' : 'Hidalgo: Central de Abasto de Pachuca',
        '140' : 'Jalisco: Mercado de Abasto de Guadalajara',
        '151' : 'México: Central de Abasto de Ecatepec',
        '150' : 'México: Central de Abasto de Toluca',
        '160' : 'Michoacán: Mercado de Abasto de Morelia',
        '170' : 'Morelos: Central de Abasto de Cuautla',
        '180' : 'Nayarit: Mercado de abasto "Adolfo López Mateos" de Tepic',
        '181' : 'Nayarit: Nayarabastos de Tepic',
        '191' : 'Nuevo León: Central de Abasto de Guadalupe, Nvo. León',
        '190' : 'Nuevo León: Mercado de Abasto "Estrella" de San Nicolás de los Garza',
        '200' : 'Oaxaca: Módulo de Abasto de Oaxaca',
        '210' : 'Puebla: Central de Abasto de Puebla',
        '220' : 'Querétaro: Mercado de Abasto de Querétaro',
        '230' : 'Quintana Roo: Mercado de Chetumal, Quintana Roo',
        '240' : 'San Luis Potosí: Centro de Abasto de San Luis Potosí',
        '250' : 'Sinaloa: Central de Abasto de Culiacán',
        '261' : 'Sonora: Central de Abasto de Cd. Obregón',
        '260' : 'Sonora: Mercado de Abasto "Francisco I. Madero" de Hermosillo',
        '270' : 'Tabasco: Central de Abasto de Villahermosa',
        '281' : 'Tamaulipas: Módulo de Abasto de Reynosa',
        '280' : 'Tamaulipas: Módulo de Abasto de Tampico, Madero y Altamira',
        '302' : 'Veracruz: Central de Abasto de Minatitlán',
        '306' : 'Veracruz: Mercado Malibrán',
        '307' : 'Veracruz: Otros Centros Mayoristas de Xalapa',
        '304' : 'Veracruz: Otros puntos de cotización en Poza Rica, Ver.',
        '310' : 'Yucatán: Central de Abasto de Mérida',
        '320' : 'Zacatecas: Mercado de Abasto de Zacatecas'
        }


    url_base = 'http://www.economia-sniim.gob.mx/Nuevo/Consultas/MercadosNacionales/PreciosDeMercado/Agricolas/ResultadoConsultaMensualGranos'

    tipo = 'Granos'

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
    results = []

    # Iterate over years, months, hidrologyc mode and cicle (otonio-invierno
    # or primavera-verano)
    for year, month, central in product(anios, meses, centrales_dict.keys()):

        # Test for dates that are yet to occur
        if (month < end_month or year < end_year) and (month >= start_month or year > start_year):
            print('Retrieving year={}, month={}, tipo={}, central={}'.format(year, month, tipo, centrales_dict[central]))

            # Create payload to post
            url = url_base + '.aspx?Anio={}&Mes={}&DestinoId={}'.format(year, month, central)

            # Get post response
            try:
                response = requests.get(url)
            except Exception:
                logging.info('Connection error: year={}, month={}, tipo={}, central={}'.format(year, month, tipo, centrales_dict[central]))
                print('Connection error: year={}, month={}, tipo={}, central={}'.format(year, month, tipo, centrales_dict[central]))
                response = False

            # Test for response
            if response:
                print('Successful response!')

                # Get information table from HTLM response
                soup = BeautifulSoup(response.text, 'html.parser')
                table = soup.find('table', attrs={
                                  'id': 'tblResultados'})

                # Iterate over table rows and extract information. Since the response lacks 'estado' for
                # a state's second and subsequent occurances, we add 'estado' with the
                # help of a boolean variable 'keep' and  a response variable
                # 'keep_state'
                if table:
                    print(':D       Table found')
                    records = []

                    # Iterate over rows
                    for row in table.findAll('tr'):

                        td1 = row.find_all('td', attrs={'class' : 'datos2'})
                        td2 = row.find_all('td', attrs={'class':'DatosNum2'}) #DatosNum2
                        tds = td1 + td2
                        # Table format contains summaries of the data in the middle of the table;
                        # since they are not <td>, we can simply test for their
                        # absence

                        if tds:

                            records.append(
                                [' '.join(elem.text.lower().split()) for elem in tds])

                    # Check that there are indeed five columns (for five weeks a month)
                    records = five_weeks(table, records, tipo)

                    # Add month, year, central de abasto
                    for row in records:
                        row.extend([month, year, central])

                    # Add successful response to the main table
                    results.extend(records)
                else:
                    print(':/       No table found')

    col_names = ['producto', 'origen', 'sem_1', 'sem_2', 'sem_3', 'sem_4',
    'sem_5', 'prom_mes', 'mes','anio', 'central']
    
    # Write file trto csv
    if output:
        file_name = output
    else:
        file_name = '../data/' + start_date + 'granos'
    
    if results:

        result = pd.DataFrame(results, columns=col_names)
        result.to_csv(file_name +'.csv', index=False)

    else:

        file = open(file_name,'w')
        file.close()
        file = open('missing.txt','w')
        file.write(file_name)
        file.close()

    return pd.DataFrame(results, columns=col_names)


def ingest_frutos(start_date, end_date='', output=''):
    """

    Creates a CSV file with weekly prices from centrales de abasto. 

    Args:
        (start_date): format po'year-m'
        (end_date) : format 'year-m'. If None, only start_date is downloaded
        (output): file name for the CSV 

    Returns:
        None. Writes a CSV file.
    """
    if not path.exists('logs'):
        makedirs('logs')
    logging.basicConfig(filename='logs/precios-frutos.log', level=logging.DEBUG)

    # Note: we scrape by central because 'Todos' option doesn't always yield all of the centrales
    centrales_dict = {
        '10' : 'Aguascalientes: Centro Comercial Agropecuario de Aguascalientes',
        '33' : 'Baja California : Central de Abasto INDIA, Tijuana',
        '20' : 'Baja California Sur: Unión de Comerciantes de La Paz',
        '40' : 'Campeche: Mercado "Pedro Sáinz de Baranda", Campeche',
        '50' : 'Coahuila: Central de Abasto de La Laguna, Torreón',
        '80' : 'Colima: Centros de distribución de Colima',
        '70' : 'Chiapas: Central de Abasto de Tuxtla Gutiérrez',
        '61' : 'Chihuahua: Central de Abasto de Chihuahua',
        '63' : 'Chihuahua: Mercado de Abasto de Cd. Juárez',
        '100' : 'DF: Central de Abasto de Iztapalapa DF',
        '102' : 'Durango: Central de Abasto "Francisco Villa"',
        '101' : 'Durango: Centro de Distribución y Abasto de Gómez Palacio',
        '110' : 'Guanajuato: Central de Abasto de León',
        '112' : 'Guanajuato: Mercado de Abasto de Celaya ("Benito Juárez")',
        '111' : 'Guanajuato: Módulo de Abasto Irapuato',
        '121' : 'Guerrero: Central de Abastos de Acapulco',
        '130' : 'Hidalgo: Central de Abasto de Pachuca',
        '140' : 'Jalisco: Mercado de Abasto de Guadalajara',
        '141' : 'Jalisco: Mercado Felipe Ángeles de Guadalajara',
        '151' : 'México: Central de Abasto de Ecatepec',
        '150' : 'México: Central de Abasto de Toluca',
        '160' : 'Michoacán: Mercado de Abasto de Morelia',
        '170' : 'Morelos: Central de Abasto de Cuautla',
        '172' : 'Morelos: Mercado "Adolfo López Mateos" de Cuernavaca',
        '180' : 'Nayarit: Mercado de abasto "Adolfo López Mateos" de Tepic',
        '190' : 'Nuevo León: Mercado de Abasto "Estrella" de San Nicolás de los Garza',
        '200' : 'Oaxaca: Módulo de Abasto de Oaxaca',
        '210' : 'Puebla: Central de Abasto de Puebla',
        '220' : 'Querétaro: Mercado de Abasto de Querétaro',
        '230' : 'Quintana Roo: Mercado de Chetumal, Quintana Roo',
        '231' : 'Quintana Roo: Módulo de Abasto Cancún',
        '240' : 'San Luis Potosí: Centro de Abasto de San Luis Potosí',
        '250' : 'Sinaloa: Central de Abasto de Culiacán',
        '261' : 'Sonora: Central de Abasto de Cd. Obregón',
        '260' : 'Sonora: Mercado de Abasto "Francisco I. Madero" de Hermosillo',
        '270' : 'Tabasco: Central de Abasto de Villahermosa',
        '281' : 'Tamaulipas: Módulo de Abasto de Reynosa',
        '280' : 'Tamaulipas: Módulo de Abasto de Tampico, Madero y Altamira',
        '301' : 'Veracruz: Central de Abasto de Jalapa',
        '302' : 'Veracruz: Central de Abasto de Minatitlán',
        '306' : 'Veracruz: Mercado Malibrán',
        '304' : 'Veracruz: Otros puntos de cotización en Poza Rica, Ver.',
        '310' : 'Yucatán: Central de Abasto de Mérida',
        '311' : 'Yucatán: Centro Mayrista Oxkutzcab',
        '312' : 'Yucatán: Mercado "Casa del Pueblo"',
        '320' : 'Zacatecas: Mercado de Abasto de Zacatecas'
        }
    
    url_base = 'http://www.economia-sniim.gob.mx/nuevo/Consultas/MercadosNacionales/PreciosDeMercado/Agricolas/ResultadoConsultarMensualFrutasYHortalizas'

    tipo = 'FrutosYHortalizas'
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
    results = []

    # Iterate over years, months, hidrologyc mode and cicle (otonio-invierno
    # or primavera-verano)
    for year, month, central in product(anios, meses, centrales_dict.keys()):

        # Test for dates that are yet to occur
        if (month < end_month or year < end_year) and (month >= start_month or year > start_year):
            print('Retrieving year={}, month={}, tipo={}, central={}'.format(year, month, tipo, centrales_dict[central]))

            # Create payload to post
            url = url_base + '.aspx?Anio={}&Mes={}&DestinoId={}'.format(year, month, central)

            # Get response
            try:
                response = requests.request(method='GET', url=url, timeout=30)
            except Exception:
                logging.info('Connection error: year={}, month={}, tipo={}, central={}'.format(year, month, tipo, centrales_dict[central]))
                print('Connection error: year={}, month={}, tipo={}, central={}'.format(year, month, tipo, centrales_dict[central]))
                response = False

            # Test for response
            if response:
                print("Successful response! Wubba lubba dub dub!")

                # Get information table from HTLM response
                soup = BeautifulSoup(response.text, 'html.parser')
                table = soup.find('table', attrs={
                                  'id': 'tblResultados'})
                rows = {}
                # Iterate over table rows and extract information. Since the response lacks 'estado' for
                # a state's second and subsequent occurances, we add 'estado' with the
                # help of a boolean variable 'keep' and  a response variable
                # 'keep_state'
                if table:
                    print(':D       Table found')
                    records = []
                    # Iterate over rows
                    for row in table.findAll('tr'):
                        # weird html. Sometimes tds are repeated, so we limit to nine
                        # all repeated are also found afterwards
                        tds = row.find_all('td', attrs={'class' : 'datos'}, limit=9) 
                        if tds:
                            records.append(
                                [' '.join(elem.text.lower().split()) for elem in tds])

                    # Check o records, msg: 'no hay registros'
                    if 'no hay registro' in records[0][0]:
                        records = []
                    # Check that there are indeed five columns (for five weeks a month)
                    else:
                        records = five_weeks(table, records, tipo)

                    # Add month, year, central de abasto
                    for row in records:
                        row.extend([month, year, central])

                    # Add successful response to the main table
                    results.extend(records)
                else:
                    print(':/       No table found')
            else:
                logging.info('Connection timeout: year={}, month={}, tipo={}, central={}'.format(year, month, tipo, central))
                print('**Connection timeout**: year={}, month={}, tipo={}, central={}'.format(year, month, tipo, centrales_dict[central]))


    col_names = ['producto', 'calidad', 'presentacion', 'origen', 'sem_1', 'sem_2', 'sem_3', 'sem_4',
    'sem_5', 'mes','anio', 'central']
    
    # Write file trto csv
    if output:
        file_name = output
    else:
        file_name = '../data/' + start_date + '_frutos'
    
    if results:

        result = pd.DataFrame(results, columns=col_names)
        result.to_csv(file_name, index=False)

    else:

        file = open(file_name + '.csv','w')
        file.close()
        file = open('missing.txt','w')
        file.write(file_name)
        file.close()

    return pd.DataFrame(results, columns=col_names)



def five_weeks(table, records, tipo):
    """
        Checks that there are 5 week columns per month. If not, 
        fills in the necessary weeks as '--' 
    """
    if 'Grano' in tipo:
        start = 2
        week_5 = table.find_all('td', attrs={'class':'titDATTab2'})
    else:
        start = 4
        week_5 = table.find_all('td', attrs={'class':'titDATTab'})

    week_5 = [entry.text for entry in week_5]
    for i in range(start, start + 5):
        
        if not str(str(i-start+1) + 'a Semana') in week_5[i]:
            week_5.insert(i, str(i-start+1) + 'a Semana')
            for row in records:
                row.insert(i,'--')   

    return records 


if __name__ == '__main__':

    import argparse
    parser = argparse.ArgumentParser(description="Download the Secretariat of Economy's \
        Prices for different centrales de abasto")
    
    parser.add_argument('start', type=str, default='2004-1',
        help= 'First month to download, as string format yyyy-m')
    parser.add_argument('--end', type=str, default='',
        help= 'Last month to download, as string format yyyy-m. If None, \
        only download the month specified in start')
    parser.add_argument('--frutos', type=bool, default=False,
        help = 'Name of outputfile')
    parser.add_argument('--output', type=str, default='',
        help = 'Name of outputfile')
    
    args = parser.parse_args()
    
    start_date = args.start
    end_date = args.end
    frutos = args.frutos
    output = args.output

    if not frutos:
        ingest_precios(start_date=start_date, end_date=end_date, output=output)
    else:
        ingest_frutos(start_date=start_date, end_date=end_date, output=output)
