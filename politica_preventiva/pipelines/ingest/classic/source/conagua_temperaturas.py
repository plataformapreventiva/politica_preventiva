# coding=utf-8
import argparse
import calendar
import requests
import pdb
import xmltodict
import pandas as pd

from datetime import datetime
from bs4 import BeautifulSoup as bS


def genera_fechas(data_date):
    anio = data_date[0:4]
    mes = data_date[5:]
    dias = calendar.monthrange(int(anio), int(mes))[1]

    if int(mes) < 10:
        mes = '0' + str(mes)
    fechas = list()
    for i in range(0, dias):
        dia = i + 1
        if dia < 10:
            dia = '0' + str(dia)

        fechas.append(anio + '/' + mes + '/' + str(dia))

    hoy = datetime.today()

    if hoy.year == int(anio) and hoy.month == int(mes):
        fechas = fechas[0:(hoy.day-1)]

    return fechas


def conagua_temperaturas(local_path='', local_ingest_file='', data_date=''):

    """
    Función que descarga de la página de https://correo1.conagua.gob.mx/google/Google.asmx
    datos de temperatura.

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
                        <TemperaturaDiariaGrupo xmlns="https://correo1.conagua.gob.mx/Google/">
                             <dteFecha>{fecha}</dteFecha>
                        </TemperaturaDiariaGrupo>
                    </soap12:Body>
                </soap12:Envelope>
            """

    fechas = genera_fechas(data_date)
    temperaturas = pd.DataFrame()
    for fecha in fechas:
        body_f = body.format(fecha=fecha)
        response = requests.post(url, data=body_f, headers=headers)
        contenido = response.content
        soup = bS(contenido, "lxml")
        resultado_xml = soup.find('temperaturadiariagruporesult')
        resultado_hijo = next(resultado_xml.children)
        aux = resultado_hijo.encode('utf-8').strip()
        o = xmltodict.parse(aux)
        xml_aux = o.popitem()
        tupl_aux = xml_aux[1].popitem()
        dict_aux = tupl_aux[1]
        data = pd.DataFrame(dict_aux)
        temperaturas = temperaturas.append(data, ignore_index=True)

    # Write csv file
    if len(temperaturas) > 0 and local_ingest_file != '' and local_path != '':
        temperaturas.to_csv(local_ingest_file, sep='|', index=False)


if __name__ == '__main__':
    # Get Arguments from bash.
    parser = argparse.ArgumentParser(description='Download Conagua temperaturas')
    parser.add_argument('--data_date', type=str, help='Data date')
    parser.add_argument('--local_path', type=str, help='Local path')
    parser.add_argument('--local_ingest_file', type=str, help='Local ingest file')
    args = parser.parse_args()
    _local_path = args.local_path
    _local_ingest_file = args.local_ingest_file
    _data_date = args.data_date

    conagua_temperaturas(local_path=_local_path,
                          local_ingest_file=_local_ingest_file,
                          data_date=_data_date)
