#!/usr/bin/env python
"""
Ingesta de datos de delitos del fuero común.
Fuente: SESNP

Este script descarga los datos de Delitos del Fuero Común, a nivel municipal.
Los datos están disponibles en una liga indicada por el source_url de este
script, el cual descarga un archivo comprimido .zip
"""

import argparse
import numpy as np
import os
import pandas as pd
import re
import urllib.request
import zipfile

def delitos_comun_municipal():
    '''
    Imports data from SENSP
    :param year
    :type data_date:

    '''

    base_url = 'http://secretariadoejecutivo.gob.mx/docs/pdfs/nueva-metodologia/Municipal-Delitos-2015-2017.zip'

    # Importante: necesito un test que verifique que, dado un data-date,
    # existan datos para todos los estados, y si no entonces truene

if __name__ == '__main__':
    # Get Arguments from bash.
    parser = argparse.ArgumentParser(description= 'Download data from SENSP:
            delitos del fuero común')
    parser.add_argument('--data_date', type=int, help='Data date')
    parser.add_argument('--local_path', type=str, help='Local path')
    parser.add_argument('--local_ingest_file', type=str, help='Local ingest file')
    args = parser.parse_args()

    _data_dir = args.data_dir
    _local_path = args.local_path
    _local_ingest_file = args.local_ingest_file

    data = delitos_compun_municipal()




