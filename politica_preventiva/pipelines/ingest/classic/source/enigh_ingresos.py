#!/usr/bin/env python

import pdb
import argparse

import pandas as pd
from io import BytesIO
from zipfile import ZipFile
from urllib.request import urlopen


NAMES = ["folioviv","foliohog","numren","clave","mes_1","mes_2","mes_3",
         "mes_4","mes_5","mes_6","ing_1","ing_2","ing_3","ing_4","ing_5",
         "ing_6","ing_tri"]


def read_file(year):
    path = 'http://www.beta.inegi.org.mx/contenidos/proyectos/enchogares/regulares/enigh/nc/'
    if year == '2016':
        name_file = '2016/microdatos/enigh2016_ns_ingresos_csv.zip'
        name_csv = 'ingresos.csv'
    elif year == '2014':
        name_file = '2014/microdatos/NCV_Ingresos_2014_concil_2010_csv.zip'
        name_csv = 'ncv_ingresos_2014_concil_2010.csv'
    elif year == '2012':
        name_file = '2012/microdatos/NCV_Ingresos_2012_concil_2010_csv.zip'
        name_csv = 'ncv_ingresos_2012_concil_2010_csv.csv'
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
    parser = argparse.ArgumentParser(description="Download ENIGH poblacion")
    parser.add_argument('--data_date', type=str, default='na',
                        help='This pipeline does not have a data date')
    parser.add_argument('--data_dir', type=str, default='/data/enigh_hogares',
                        help='Local path of ingest data')
    parser.add_argument('--local_ingest_file', type=str, default='',
                        help='Name of output file')

    args = parser.parse_args()

    df = read_file(args.data_date)
    df.to_csv(args.local_ingest_file, sep='|', index=False)
