#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
Cleaning Functions for avance agricola dataset
"""

import pandas as pd
import numpy as np
import re
from difflib import SequenceMatcher

edo_dict = pd.read_pickle('utils/edos_dict.p')
muns_dict = pd.read_pickle('utils/muns_dict.p')

def remove_accent(x):
    subs = [('á', 'a'),
       ('é', 'e'),
       ('í', 'i'),
       ('ó', 'o'),
       ('ú', 'u'),
       ('Á', 'A'),
       ('É', 'E'),
       ('Í', 'I'),
       ('Ó', 'O'),
       ('Ú', 'U'),
       ('ñ', 'n'),
       ('Ñ', 'n'),
       ('¥', 'n')]
    if pd.isnull(x):
        return None
    if isinstance(x, str):
        x_2 = x
        for tup_subs in subs:
            x_2 = re.sub(tup_subs[0], tup_subs[1], x_2)
        return x_2
    else:
        return None

def get_estado_key(x):
    try:
        return edo_dict[x]
    except KeyError:
        return x

def str_to_int(x, missing='zero'):
    if isinstance(x, str):
        x = re.sub(',','',x)
    try:
        return int(x)
    except ValueError:
        if missing == 'zero':
            return 0
        else:
            return None

def str_to_float(x, missing='zero'):
    if isinstance(x, str):
        x = re.sub(',','',x)
    try:
        return int(x)
    except ValueError:
        if missing == 'zero':
            return 0
        else:
            return None


###### PULIR get_municipio_key: algunos no se homologan bien
def get_municipio_key(nom, edo):
    if nom:
        nom = remove_accent(nom)
        try:
            cve_mun = muns_dict[edo][nom]
        except:
            keys = [b for b in muns_dict[edo].keys()]
            max_mun = keys[0]
            r_max = SequenceMatcher(None, max_mun, nom).ratio()
            for mun in muns_dict[edo].keys():
                r = SequenceMatcher(None, mun, nom).ratio()
                if r > .55 and r >= r_max:
                    r_max = r
                    max_mun = mun
            if r_max > .75:
                cve_mun = muns_dict[edo][max_mun]
            else:
                cve_mun = None
    else:
        cve_mun = None
    return cve_mun

def get_temporal_data_maiz(mes, anio, ciclo):
    """
        Function to reframe temporal data into agricultural terms
        An anio agricola n is defined as spanning from october n-1 
        to march n+1, depending on cicle. 
        Input.
        (anio) 
        (ciclo)
        (mes)
        as obtained from get_avance_agricola in utilsAPI
        Output:
        (anio_ag): agricultural year
        (mes_ag): number of agricultural month:
            FOR otonio-invierno cycle:
            [oct n-1 to sep n]
            FOR primvera-verano
            [apr n to march n+1]
    """
    ######## anadir datos de prod y superficie siniestrada para 
    ######## ver realmente de donde viene el dato de 
    if ciclo == 'OI':
        if int(mes) > 9:
            mes_ag = int(mes) - 9
            anio_ag = int(anio) + 1
        else:
            mes_ag = int(mes) + 3
            anio_ag = int(anio)
    else:
        if int(mes) < 4:
            mes_ag = int(mes) + 9
            anio_ag = int(anio) - 1
        else:
            mes_ag = int(mes) - 3
            anio_ag = int(anio) 
    return mes_ag, anio_ag

producto = 'maiz'
path = 'data/carencia-alimenticia/avance_agricola_' + producto + '.p'

avance = pd.read_pickle(path)
avance = avance.drop('distrito',1)
avance['estado'] = list(map(lambda x: get_estado_key(x), avance['estado']))
avance['municipio'] = avance.apply(lambda x: get_municipio_key(x.municipio, x.estado), axis = 1)
avance['sup_sembrada'] = list(map(lambda x: str_to_int(x), avance['sup_sembrada']))
avance['sup_cosech'] = list(map(lambda x: str_to_int(x), avance['sup_cosech'])) 
avance['sup_siniest'] = list(map(lambda x: str_to_int(x), avance['sup_siniest']))
avance['produccion'] = list(map(lambda x: str_to_int(x), avance['produccion']))
avance['rendim'] = list(map(lambda x: str_to_float(x), avance['rendim']))
##### NOTA: hay municipios que se encuentran en varios distritos, por lo que
##### estamos sumamos sobre ellos en el sig. paso. REVISAR si esta bien
avance = avance.groupby(['estado', 'municipio', 'mes', 'anio', 
    'moda_hidr', 'ciclo']).sum()
avance = avance.reset_index()
avance[['mes_g', 'anio_g']] = pd.DataFrame(np.array(avance[['mes', 'anio', 'ciclo']].apply(lambda x: 
    get_temporal_data_maiz(x.mes, x.anio, x.ciclo), axis=1)).tolist(), index=avance.index, columns=['mes_g', 'anio_g'])

path_save = 'data/carencia-alimenticia/avance_agricola_' + producto + '_clean.p'
avance.to_pickle(path_save)