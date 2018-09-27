#!/usr/bin/env python

import pdb
import argparse

import pandas as pd
from io import BytesIO
from zipfile import ZipFile
from urllib.request import urlopen


NAMES = ["folioviv","foliohog","numren","id_trabajo","trapais","subor","indep","personal","pago","contrato",
         "tipocontr","pres_1","pres_2","pres_3","pres_4","pres_5","pres_6","pres_7","pres_8","pres_9",
         "pres_10","pres_11","pres_12","pres_13","pres_14","pres_15","pres_16","pres_17","pres_18",
         "pres_19","pres_20","pres_21","pres_22","pres_23","pres_24","pres_25","pres_26",
         "htrab","sinco","scian","clas_emp","tam_emp","no_ing","tiene_suel","tipoact","socios","soc_nr1",
         "soc_nr2","soc_resp","otra_act","tipoact2","tipoact3","tipoact4","lugar","conf_pers",
         "reg_not","reg_cont","com_fis"]

def read_file(year):
    path = 'http://www.beta.inegi.org.mx/contenidos/proyectos/enchogares/regulares/enigh/nc/'
    if year == '2016':
        name_file = '2016/microdatos/enigh2016_ns_trabajos_csv.zip'
        name_csv = 'trabajos.csv'
    elif year == '2014':
        name_file = '2014/microdatos/NCV_Trabajos_2014_concil_2010_csv.zip'
        name_csv = 'ncv_trabajos_2014_concil_2010.csv'
    elif year == '2012':
        name_file = '2012/microdatos/NCV_Trabajos_2012_concil_2010_csv.zip'
        name_csv = 'ncv_trabajos_2012_concil_2010_csv.csv'
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
