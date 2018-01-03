# coding=utf-8
import argparse
import json
import pdb
import requests
import pandas as pd

from bs4 import BeautifulSoup as bS


def conagua_estaciones(local_path='', local_ingest_file='', data_date=''):

    """
    Función que descarga de la página de https://correo1.conagua.gob.mx/google/Google.asmx
    todas las estaciones climatológicas que existen en ese momento.

    Args:
        local_path (str): string with path to which local file should be saved.
        local_ingest_file (str): string with name of csv.
        data_date (str): string with date of pipeline task.

    Returns:
        True if task is completed
            It also saves a DataFrame with all estaciones climatológicas from Conagua
            in output as csv with sep='|'.

    """

    url = "https://correo1.conagua.gob.mx/google/Google.asmx?WSDL"
    headers = {'content-type': 'application/soap+xml; charset=utf-8'}
    body = """<?xml version="1.0" encoding="utf-8"?>
                    <soap12:Envelope xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance" xmlns:xsd="http://www.w3.org/2001/XMLSchema" xmlns:soap12="http://www.w3.org/2003/05/soap-envelope">
                    <soap12:Body>
                    <CatalogoEstaciones xmlns="https://correo1.conagua.gob.mx/Google/" />
                        </soap12:Body>
                    </soap12:Envelope>
                """

    response = requests.post(url, data=body, headers=headers)
    contenido = response.content
    soup = bS(contenido, "lxml")
    resultado_html = soup('catalogoestacionesresult')[0]
    resultado_hijo = next(resultado_html.children)
    resultado_str = resultado_hijo.encode('utf-8').strip()
    resultado_json = json.loads(resultado_str)
    num_estaciones = len(resultado_json)
    estaciones = pd.DataFrame()
    for i in range(0, num_estaciones):
        dict_aux = resultado_json[i]
        serie_aux = pd.Series(dict_aux)
        estaciones = estaciones.append(serie_aux, ignore_index=True)
    

    # Write csv file
    if (len(estaciones) > 0 and local_ingest_file != '' and
            local_path != '' and data_date in [2010, 2018]):
        estaciones.to_csv(local_ingest_file, sep='|', index=False)


if __name__ == '__main__':
    # Get Arguments from bash.
    parser = argparse.ArgumentParser(description='Download Conagua estaciones climatológicas')
    parser.add_argument('-data_date', type=int, help='Data date')
    parser.add_argument('-local_path', type=str, help='Local path')
    parser.add_argument('-local_ingest_file', type=str, help='Local ingest file')
    args = parser.parse_args()
    _local_path = args.local_path
    _local_ingest_file = args.local_ingest_file
    _data_date = args.data_date

    conagua_estaciones(local_path=_local_path,
                       local_ingest_file=_local_ingest_file,
                       data_date=_data_date)

