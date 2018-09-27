#!/usr/bin/env python

import pdb
import argparse

import pandas as pd
from io import BytesIO
from zipfile import ZipFile
from urllib.request import urlopen


NAMES = ["folioviv","foliohog","numren","clave","tipo_gasto","mes_dia",
         "frec_rem","inst","forma_pago","forma_pag1","forma_pag2",
         "forma_pag3","inscrip","colegia","material","cantidad","gasto",
         "costo","gasto_tri","gasto_nm","gas_nm_tri"]


def read_file(year):
    path = 'http://www.beta.inegi.org.mx/contenidos/proyectos/enchogares/regulares/enigh/nc/'
    if year == '2016':
        name_file = '2016/microdatos/enigh2016_ns_gastospersona_csv.zip'
        name_csv = 'gastospersona.csv'
    elif year == '2014':
        name_file = '2014/microdatos/NCV_Gastopersona_2014_concil_2010_csv.zip'
        name_csv = 'ncv_gastopersona_2014_concil_2010.csv'
    elif year == '2012':
        name_file = '2012/microdatos/NCV_Gastopersona_2012_concil_2010_csv.zip'
        name_csv = 'ncv_gastopersona_2012_concil_2010_csv.csv'
    else:
        print('No information for that year')
        sys.exit()

    resp = urlopen(path + name_file)
    with ZipFile(BytesIO(resp.read())) as z:
        with z.open(name_csv) as f:
            df = pd.read_csv(f)
    df = df.reindex(columns= NAMES)
    return df

if __name__ == '__main__':
    parser = argparse.ArgumentParser(description="Download ENIGH hogares")
    parser.add_argument('--data_date', type=str, default='na',
                        help='This pipeline does not have a data date')
    parser.add_argument('--data_dir', type=str, default='/data/enigh_hogares',
                        help='Local path of ingest data')
    parser.add_argument('--local_ingest_file', type=str, default='',
                        help='Name of output file')

    args = parser.parse_args()

    df = read_file(args.data_date)
    df.to_csv(args.local_ingest_file, sep='|', index=False)
