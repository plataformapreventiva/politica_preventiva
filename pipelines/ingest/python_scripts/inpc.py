#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""Utilidades Cenapred

Funciones de Descarga y limpieza de Task Cenapred
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


def ingest_inpc_ciudad(single_year='2017', historic_until=None,
 output=None):
    """
    Returns a Pandas with IPC information from INEGI services

    Args:
        (single_year): Download info from a single year
        (historic_until): To download historic data from year 1969.
        (output): CSV where to write the results

    Returns:
        Returns two objects
        - Pandas dataframe data
        - metadata: .

    e.g.
        - data, metadata = get_inpc_ciudad_data()

    """

    data = pd.DataFrame()
    metadata = {}

    #dict of ciudades with state code
    dict_ciudades = {
        "Area Metropolitana de la Cd. de México":"09",
        "Acapulco,%20Gro.":"12",
        "Aguascalientes, Ags.":"01",
        "Campeche, Camp.":"04",
        "Cd. Acuña, Coah.":"05",
        "Cd. Jiménez, Chih.":"08",
        "Cd. Juárez, Chih.":"08",
        "Colima, Col.":"06",
        "Córdoba, Ver.":"30",
        "Cortazar, Gto.":"11",
        "Cuernavaca, Mor.":"17",
        "Culiacán, Sin.":"25",
        "Chetumal, Q.R.":"23",
        "Chihuahua, Chih.":"08",
        "Durango, Dgo.":"10",
        "Fresnillo, Zac.":"32",
        "Guadalajara, Jal.":"14",
        "Hermosillo, Son.":"26",
        "Huatabampo, Son.":"26",
        "Iguala, Gro.":"12",
        "Jacona, Mich.":"16",
        "La Paz, B.C.S.":"03",
        "León, Gto.":"11",
        "Matamoros, Tamps.":"28",
        "Mérida, Yuc.":"31",
        "Mexicali, B.C.":"02",
        "Monclova, Coah.":"05",
        "Monterrey, N.L.":"19",
        "Morelia, Mich.":"16",
        "Oaxaca, Oax.":"20",
        "Puebla, Pue.":"21",
        "Querétaro, Qro.":"22",
        "San Andrés Tuxtla, Ver.":"30",
        "San Luis Potosí, S.L.P.":"24",
        "Tampico, Tamps.":"28",
        "Tapachula, Chis.":"07",
        "Tehuantepec, Oax.":"20",
        "Tepatitlán, Jal.":"14",
        "Tepic, Nay.":"18",  #error downloading data from tepic
        "Tijuana, B.C.":"02",
        "Tlaxcala, Tlax.":"29",
        "Toluca, Edo. de Méx.":"15",
        "Torreón, Coah.":"05",
        "Tulancingo, Hgo.":"13",
        "Veracruz, Ver.":"30",
        "Villahermosa, Tab.":"27"
        }

    cols = [
    'fecha', 
    'indice', 
    'alim_bt',
    'alim',
    'alim_pantc',
    'alim_car',
    'alim_pescm',
    'alim_lech',
    'alim_aceig',
    'alim_fruth',
    'alim_azucf',
    'alim_otr',
    'alim_alcht',
    'ropa',
    'viv',
    'mueb',
    'salu',
    'transp',
    'edu',
    'otro',
    'cmae_1',
    'cmae_2',
    'cmae_2',
    'scian_1',
    'scian_2',
    'scian_3'
    ]

    if historic_until:
        year_query = "&_anioI=1969&_anioF={0}".format(historic_until)
    else:
        year_query = "&_anioI={0}&_anioF={0}".format(single_year)

    for ciudad in dict_ciudades:
        ciudad_encoded = ciudad.replace(" ","+")
        #.encode("utf-8")
        ciudad_id = dict_ciudades[ciudad]
        base = ("http://www.inegi.org.mx/sistemas/indiceprecios/Exportacion.aspx?INPtipoExporta=CSV"
        "&_formato=CSV")

        #tipo niveles
        tipo = ("&_meta=1&_tipo=Niveles&_info=%C3%8Dndices&_orient=vertical&esquema=0&"
            "t=%C3%8Dndices+de+Precios+al+Consumidor&")

        lugar = "st={0}".format(ciudad_encoded)

        serie = ("&pf=inp&cuadro=0&SeriesConsulta=e%7C240123%2C240124%2C240125%"
        "2C240126%2C240146%2C240160%2C240168%2C240186%2C240189%2C240234"
        "%2C240243%2C240260%2C240273%2C240326%2C240351%2C240407%2C240458"
        "%2C240492%2C240533%2C260211%2C260216%2C260260%2C320804%2C320811%2C320859%2C")

        url = base + year_query + tipo + lugar + serie


        try:
            #download metadata
            metadata[ciudad] = pd.read_csv(url,error_bad_lines=False,nrows=5,usecols=[0],header=None).values
            #download new dataframe
            print('trying to download data from {}'.format(ciudad))

            temp = pd.read_csv(url,error_bad_lines=False,skiprows=14,header=None, names=cols)
            temp['ciudad'] = ciudad

            data = pd.concat([data, temp])
            print("Query succesful for city {}".format(ciudad))

        except:
            print ("Error downloading data for : {}".format(ciudad))

    if output:
        data.to_csv(output)
    else:
        return data, metadata


# cols = {
# 'indice':'Indice general',
# 'alim_bt':'Alimentos, bebidas y tabaco',
# 'alim':'Alimentos',
# 'alim_pantc':'Pan, tortillas y cereales',
# 'alim_car':'Carnes',
# 'alim_pescm':'Pescados y mariscos',
# 'alim_lech':'Leche, derivados de leche y huevo',
# 'alim_aceig':'Aceites y grasas comestibles',
# 'alim_fruth':'Frutas y hortalizas',
# 'alim_azucf':'Azucar, cafe y refrescos envasados',
# 'alim_otr':'Otros alimentos',
# 'alim_alcht':'Bebidas alcoholicas y tabaco',
# 'ropa':'Ropa, calzado y accesorios',
# 'viv':'Vivienda',
# 'mueb':'Muebles, aparatos y accesorios domesticos',
# 'salu':'Salud y cuidado personal',
# 'transp':'Transporte',
# 'edu':'Educacion y esparcimiento',
# 'otro':'Otros servicios',
# 'cmae_1':'CMAE: Sector economico primario',
# 'cmae_2':'CMAE: Sector economico secundario',
# 'cmae_2':'CMAE: Sector economico terciario',
# 'scian_1':'SCIAN: Sector economico primario',
# 'scian_2':'SCIAN: Sector economico secundario',
# 'scian_3':'SCIAN: Sector economico terciario'
# }

