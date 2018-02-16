# coding=utf-8
import argparse
import calendar
import requests
import pdb
import ast
import xmltodict
import pandas as pd

import datetime
from bs4 import BeautifulSoup as bS


def genera_fechas(data_date):
    anio = data_date.split('-')[0]
    anio_dia = data_date.split('-')[1]
    fecha = datetime.datetime(int(anio), 1, 1) + datetime.timedelta(int(anio_dia) - 1)

    hoy = datetime.datetime.today()
    if fecha.date() == hoy.date():
        horas = hoy.hour - 1
    else:
        horas = 24

    fechas = [(fecha + datetime.timedelta(hours=i)).strftime("%Y/%m/%d %H:%M:%S")
                for i in range(0,horas)]

    return fechas


def conagua_dirviento(local_path='', local_ingest_file='', data_date=''):

    """
    Función que descarga de la página de https://correo1.conagua.gob.mx/google/Google.asmx
    datos de dirección del viento.

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
                        <DireccionVientoHoraria xmlns="https://correo1.conagua.gob.mx/Google/">
                             <dteFecha>{fecha}</dteFecha>
                        </DireccionVientoHoraria>
                    </soap12:Body>
                </soap12:Envelope>
            """

    fechas = genera_fechas(data_date)
    humedad = pd.DataFrame()
    for fecha in fechas:
        body_f = body.format(fecha=fecha)
        response = requests.post(url, data=body_f, headers=headers)
        contenido = response.content
        soup = bS(contenido, "lxml")
        resultado_xml = soup.find('direccionvientohorariaresult')
        resultado_hijo = next(resultado_xml.children)
        dicts_aux = ast.literal_eval(resultado_hijo)
        #aux = resultado_hijo.encode('utf-8').strip()
        data = pd.DataFrame(dicts_aux)
        humedad = humedad.append(data, ignore_index=True)

    # Write csv file
    if len(humedad) > 0 and local_ingest_file != '' and local_path != '':
        humedad.to_csv(local_ingest_file, sep='|', index=False)


if __name__ == '__main__':
    # Get Arguments from bash.
    parser = argparse.ArgumentParser(description='Download Conagua Dirección Viento horaria')
    parser.add_argument('--data_date', type=str, help='Data date')
    parser.add_argument('--local_path', type=str, help='Local path')
    parser.add_argument('--local_ingest_file', type=str, help='Local ingest file')
    args = parser.parse_args()
    _local_path = args.local_path
    _local_ingest_file = args.local_ingest_file
    _data_date = args.data_date

    conagua_dirviento(local_path=_local_path,
                          local_ingest_file=_local_ingest_file,
                          data_date=_data_date)
