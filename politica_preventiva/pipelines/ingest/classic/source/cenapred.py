#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""Ingesta Cenapred

Funciones de Descarga y limpieza de Task Cenapred
"""

import os
from os import path, makedirs
import requests
import argparse
import numpy as np
import pandas as pd
import json
from requests.auth import HTTPDigestAuth
import datetime
from itertools import product
from bs4 import BeautifulSoup
from ftplib import FTP
import requests
import sys
import pdb

# queries for sub servicios
dict_subservicio = {
    # ANR/MuniAPP - Servicio usado en
    "MuniAPPInfo": ("&outFields=clave_edo%2CCLAVE_MU_1%2CPOBTOT_%2CNOM_MUN%2CNOM_ENT_%2CTVIVHAB_%2CSEP%2CHOSPITALES"
                    "%2CBancos%2CGasolinera%2Choteles%2CSupermerca%2CAeropuerto%2Cnum_col%2CShape_Area%"
                    "2CGVS_ISE10%2CG_RezagoSo%2CG_Marginac%2CG_Resilien%2CGP_ondasca%2CGP_Inundac%2CGP_Ciclnes%"
                    "2CGP_BajasTe%2CGP_Nevadas%2CGP_Granizo%2CGP_TormEle%2CGP_Sequia2%2CGP_Sismico%2CGP_SusTox%"
                    "2CGP_Tsunami%2CGP_SusInfl%2CG_SusceLad%2Curl%2CA_REGLAM%2CTITULO_REG%2Catlas_mun%2CCC_HIDRO%"
                    "2CD_GEO%2CD_HIDRO%2CD_QUI%2CE_GEO%2CE_HIDRO%2CE_QUI%2CE_SANI%2CDECLARATOR%2CPop2030%2CV_CC%"
                    "2CNum_Us_CFE%2CPOBFEM_%2CPOBMAS_%2COBJECTID")
                    }

def cenapred(output, servicio="ANR", subservicio="MuniAPPInfo", geometria=False):

    """
    Returns a DataFrame with municipality level information from CENAPRED
    services

    Args:
        servicio (str): Define service to query:
            List[]
        subservicio (str): Define Subservices to query from:
            List[]
    Returns:
        dataframe: Pandas Dataframe with municipality level information.

    """
    # Create logging
    if not path.exists('logs'):
        makedirs('logs')
    #logging.basicConfig(filename='logs/cenapred.log',
    #                    level=logging.DEBUG)

    # url to query
    URL = "http://servicios2.cenapred.unam.mx:6080/arcgis/rest/services/{0}/{1}/MapServer/0/query?f=json".\
        format(servicio, subservicio)

    # to query all states using where condition
    STATE = ("&where=NOM_ENT_%20IN%20(%27Aguascalientes%27%2C%27Baja%20California%27%2C%27Baja%20California%"
             "20Sur%27%2C%27Campeche%27%2C%27Chiapas%27%2C%27Chihuahua%27%2C%27Coahuila%20de%20Zaragoza%27%2C%"
             "27Colima%27%2C%27Distrito%20Federal%27%2C%27Durango%27%2C%27M%C3%A9xico%27%2C%27Guanajuato%27%2C%"
             "27Guerrero%27%2C%27Hidalgo%27%2C%27Jalisco%27%2C%27Michoac%C3%A1n%20de%20Ocampo%27%2C%27Morelos%27%"
             "2C%27Nayarit%27%2C%27Nuevo%20Le%C3%B3n%27%2C%27Oaxaca%27%2C%27Puebla%27%2C%27Quer%C3%A9taro%27%2C%"
             "27Quintana%20Roo%27%2C%27San%20Luis%20Potos%C3%AD%27%2C%27Sinaloa%27%2C%27Sonora%27%2C%27Tabasco%27%"
             "2C%27Tamaulipas%27%2C%27Tlaxcala%27%2C%27Veracruz%20de%20Ignacio%20de%20la%20Llave%27%2C%27Yucat%C3%"
             "A1n%27%2C%27Zacatecas%27)")

    # default query parameters for query
    PARAMS = ("&text=&objectIds=&time=&geometry=&geometryType=esriGeometryEnvelope&inSR=102100&spatialRel"
              "=esriSpatialRelIntersects&relationParam=")


    dict_geometry = {
        True: ("&returnGeometry=true&maxAllowableOffset=3000&geometryPrecision=%7B%22xmin%22%"
               "3A-13775786.985668927%2C%22ymin%22%3A2198940.452952425%2C%22xmax%22%3A-12523442.71424493%"
               "2C%22ymax%22%3A3451284.724376421%2C%22spatialReference%22%3A%7B%22wkid%22%3A102100%2C%"
               "22latestWkid%22%3A3857%7D%7D"),
        False: ("&returnGeometry=false&maxAllowableOffset=3000&geometryPrecision=%7B%22xmin%22%"
               "3A-13775786.985668927%2C%22ymin%22%3A2198940.452952425%2C%22xmax%22%3A-12523442.71424493%"
               "2C%22ymax%22%3A3451284.724376421%2C%22spatialReference%22%3A%7B%22wkid%22%3A102100%2C%"
               "22latestWkid%22%3A3857%7D%7D")
    }

    # Output Spatial Reference
    OUT = "&outSR=102100"
    # Return IDs Only 
    RETURN_ID = "&returnIdsOnly=true"

    # query to get all ids
    get_ids = URL + STATE + PARAMS + \
              dict_subservicio[subservicio] + \
              dict_geometry[geometria] + OUT + RETURN_ID
    myResponse_ids = requests.get(get_ids)
    myResponse_ids.raise_for_status()

    # Get ID's
    if(myResponse_ids.ok):
        # Loads (Load String) takes a Json file and converts into python data
        # structure (dict or list, depending on JSON)
        jIds = json.loads(myResponse_ids.content.decode('utf-8'))
        jIds = jIds["objectIds"]
        print('query of Ids successful - {0} ids found'.format(len(jIds)))

        # arcgis only gives 1000 ids at time
        # split the ids into groups of 1000
        chunks = int(np.ceil(len(jIds)/1000.0))
        print("splitted into {} chunks".format(chunks))
        chunks_of_ids = np.array_split(jIds, chunks)

        features = []
        i = 1
        for chunk in chunks_of_ids:
            # get id params
            objectids = ('&objectIds=+' + '+%2C+'.join(str(i) for i in chunk))
            get_dict = URL + STATE + \
                dict_geometry[geometria] + \
                dict_subservicio[subservicio] + OUT + objectids
            myResponse = requests.get(get_dict)

            if(myResponse.ok):
                # Loads (Load String) takes a Json file and converts into
                # python data structure (dict or list, depending on JSON)
                jData = json.loads(myResponse.content.decode('utf-8'))
                features.extend(jData['features'])
                print('Chunk {0} of {1}:  query successful'.format(i, chunks))
                i += 1

            else:
                # If response code is not ok (200), print the resulting http
                # error code with description
                print(myResponse.raise_for_status())

        # convert list of dicts into dataframe
        db = pd.DataFrame([x["attributes"] for x in features])

    else:
        # If response code is not ok (200), print the resulting http error code
        # with description
        print(myResponse_ids.raise_for_status())
        print("query of Id's unsuccessful")

    db.rename(columns={u'clave_edo': 'cve_ent',
                       u'CLAVE_MU_1': 'cve_muni'}, inplace=True)
    db.replace(to_replace='|', value='-', inplace=True)
    db.to_csv(output, sep='|', index=False)

    return True

if __name__ == '__main__':
    # Get Arguments from bash.
    parser = argparse.ArgumentParser(description= 'Download CENAPRED')
    parser.add_argument('--output', type=str, default='',
                        help = 'Name of outputfile')
    parser.add_argument('--servicio', type=str, default='ANR',
                        help = 'Type of service')
    parser.add_argument('--subservicio', type=str, default='MuniAPPInfo',
                        help = 'Type of subservice')
    parser.add_argument('--geometria', type=bool, default=True,
                        help = 'Add geometry')

    # Read arguments
    args = parser.parse_args()
    output = args.output
    servicio = args.servicio
    subservicio = args.subservicio
    geometria = args.geometria

    # Start function.
    cenapred(output, servicio, subservicio, geometria)
