#!/usr/bin/env python
# -*- coding: utf-8 -*-

""""
Ingesta de {datos}
Fuente: {fuente}

Este script ingesta los datos de {fuente de datos} a nivel {unidad
observacional, (i.e. nivel hogar/municipio, etc.)}

{Aclarar acá los supuestos del código. Es decir: ¿se asume que se tiene acceso
a alguna fuente de datos, como S3? ¿Se asume alguna estructura particular
para el archivo (ejemplo: "Asumimos que el nombre siempre tiene esta
forma...")}

Notas sobre este script:
1. Todos los scripts de ingesta local se guardan en pipelines/ingest/classic/source
2. Los scripts de ingesta local están pensados como el primer paso para subir
los datos crudos a nuestra base de datos, por lo que es importante hacer
solamente el procesamiento mínimo necesario para esto. Es importante recordar
que tenemos pipelines de limpieza y procesamiento de datos más adelante.
"""

# Librerías necesarias:
import argparse
import numpy as np
import pandas as pd

# Formato sugerido: definir una función que descargue los datos de su fuente
# remota y devuelva un dataframe de pandas.
# Un ejemplo:

def descargar_datos(data_dir, data_date):
    data = pd.DataFrame()
    return(data)


if __name__ == '__main__':
    # Get Arguments from bash.
    parser = argparse.ArgumentParser(description='Download listado CONEVAL: Federales')
    parser.add_argument('--data_date', type=str, help='Data date')
    parser.add_argument('--data_dir', type=str, help='Local data directory')
    parser.add_argument('--local_ingest_file', type=str, help='Local ingest file')
    args = parser.parse_args()
    _data_dir = args.data_dir
    _local_ingest_file = args.local_ingest_file
    _data_date = args.data_date

    print('Downloading data')

    data = descargar_datos(data_dir = _data_dir, data_date = _data_date)

    try:
        data.to_csv(_local_ingest_file, sep='|', index=False)
        print('Written to: {}'.format(_local_ingest_file))
    except Exception as e:
        print('Failed to write data to local ingest file')
