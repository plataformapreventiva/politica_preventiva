#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
    Descarga de CONACCA
    Confederación Nacional de Agrupaciones de Comerciantes de Centros de Abastos A.C.
    http://www.conacca.mx/index.php/styles/centrales-de-abasto

"""

import logging
import requests
import pandas as pd

from luigi import configuration
from bs4 import BeautifulSoup


# logging_conf = configuration.get_config().get("core", "logging_conf_file")
# logging.config.fileConfig(logging_conf)
# logger = logging.getLogger("dpa-sedesol")


def centrales_conacca(local_ingest_file=''):
    """

    Creates a CSV file with information of centrales de abasto.

    Args:

        (local_ingest_file): file name for the CSV

    Returns:
        None. Writes a CSV file.
    """

    print('Retrieving ')

    # Create payload to post
    url = 'http://www.conacca.mx/index.php/styles/centrales-de-abasto'

    # Tabla de resultados
    resultados = []

    # Get response
    try:
        response = requests.request(method='GET', url=url)
        print(response.status_code)
        if response:
            if int(response.status_code) == 200:
                print("Successful response! Wubba lubba dub dub!")
                # Get information table from HTLM respons
                soup = BeautifulSoup(response.text, 'html.parser')
                table = soup.find('table')
                for row in table.findAll('tr'):
                    tds = row.find_all('td', limit=9)
                    if tds:
                        resultados.append([' '.join(elem.text.split()) for elem in tds])
                        # hacer tabla de resultados
                        colnames = resultados[0]
                        datos = resultados[1:]
                        # Write file to csv
                        if local_ingest_file:
                            file_name = local_ingest_file
                        else:
                            file_name = '../data/conacca.csv'

                        if datos:
                            datos_df = pd.DataFrame(datos, columns=colnames)
                            datos_df.to_csv(file_name, index=False, sep='|')
                        else:
                            file = open(file_name, 'w')
                            file.close()
    except Exception as e:
        #logger.error('Failed to request url: ' + str(e))
        print('Connection error')


if __name__ == '__main__':
    import argparse

    parser = argparse.ArgumentParser(description="Download info from centrales de abasto")

    parser.add_argument('--data_date', type=str, default='na',
                        help='This pipeline does not have a data date')
    parser.add_argument('--data_dir', type=str, default='/data/conacca',
                        help='Local path of ingest data')
    parser.add_argument('--local_ingest_file', type=str, default='',
                        help='Name of output file')

    args = parser.parse_args()
    local_ingest_file = args.local_ingest_file

    centrales_conacca(local_ingest_file=local_ingest_file)
