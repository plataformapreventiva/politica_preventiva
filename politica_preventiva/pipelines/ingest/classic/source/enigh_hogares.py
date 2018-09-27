#!/usr/bin/env python

import pdb
import argparse

import pandas as pd
from io import BytesIO
from zipfile import ZipFile
from urllib.request import urlopen


NAMES = ['folioviv', 'foliohog', 'huespedes', 'huesp_come', 'num_trab_d', 'trab_come', 'acc_alim1', 'acc_alim2', 'acc_alim3',
         'acc_alim4', 'acc_alim5','acc_alim6','acc_alim7','acc_alim8','acc_alim9','acc_alim10','acc_alim11','acc_alim12',
        'acc_alim13', 'acc_alim14','acc_alim15', 'acc_alim16','alim17_1','alim17_2','alim17_3','alim17_4','alim17_5','alim17_6',
        'alim17_7','alim17_8','alim17_9','alim17_10','alim17_11','alim17_12','acc_alim18', 'telefono','celular', 'tv_paga',
        'conex_inte','num_auto','anio_auto','num_van','anio_van','num_pickup','anio_pickup','num_moto','anio_moto','num_bici',
        'anio_bici','num_trici','anio_trici','num_carret','anio_carret','num_canoa','anio_canoa','num_otro','anio_otro','num_ester',
        'anio_ester','num_grab','anio_grab','num_radio','anio_radio','num_tva','anio_tva','num_tvd','anio_tvd','num_dvd','anio_dvd',
        'num_video','anio_video','num_licua','anio_licua','num_tosta','anio_tosta','num_micro','anio_micro','num_refri','anio_refri',
        'num_estuf','anio_estuf','num_lavad','anio_lavad','num_planc','anio_planc','num_maqui','anio_maqui','num_venti','anio_venti',
        'num_aspir','anio_aspir','num_compu','anio_compu','num_impre','anio_impre','num_juego','anio_juego','esc_radio','er_aparato',
        'er_celular','er_compu','er_aplicac','er_tv','er_otro','recib_tvd','tsalud1_h','tsalud1_m','tsalud1_c','tsalud2_h', 'tsalud2_m',
        'habito_1','habito_2','habito_3','habito_4','habito_5','habito_6','consumo','nr_viv','tarjeta','pagotarjet','regalotar',
        'regalodado','autocons','regalos','remunera','transferen','parto_g','embarazo_g','negcua','est_alim','est_trans','bene_licon',
        'cond_licon','lts_licon','otros_lts','diconsa','frec_dicon','cond_dicon','pago_dicon','otro_pago','factor_hog']

def read_file(year):
    path = 'http://www.beta.inegi.org.mx/contenidos/proyectos/enchogares/regulares/enigh/nc/'
    if year == '2016':
        name_file = '2016/microdatos/enigh2016_ns_hogares_csv.zip'
        name_csv = 'hogares.csv'
    elif year == '2014':
        name_file = '2014/microdatos/NCV_Hogares_2014_concil_2010_csv.zip'
        name_csv = 'ncv_hogares_2014_concil_2010.csv'
    elif year == '2012':
        name_file = '2012/microdatos/NCV_Hogares_2012_concil_2010_csv.zip'
        name_csv = 'ncv_hogares_2012_concil_2010_csv.csv'
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
