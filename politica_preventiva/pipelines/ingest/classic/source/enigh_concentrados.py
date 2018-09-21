#!/usr/bin/env python

import pdb
import argparse

import pandas as pd
from io import BytesIO
from zipfile import ZipFile
from urllib.request import urlopen


NAMES = ["folioviv","foliohog","ubica_geo","ageb","tam_loc","est_socio","est_dis","upm",
         "factor_hog","clase_hog","sexo_jefe","edad_jefe","educa_jefe","tot_integ","hombres",
         "mujeres","mayores","menores","p12_64","p65mas","ocupados","percep_ing","perc_ocupa",
         "ing_cor","ingtrab","trabajo","sueldos","horas_extr","comisiones","aguinaldo","indemtrab",
         "otra_rem","remu_espec","negocio","noagrop","industria","comercio","servicios","agrope",
         "agricolas","pecuarios","reproducc","pesca","otros_trab","rentas","utilidad","arrenda",
         "transfer","jubilacion","becas","donativos","remesas","bene_gob","transf_hog","trans_inst",
         "estim_alqu","otros_ing","gasto_mon","alimentos","ali_dentro","cereales","carnes","pescado",
         "leche","huevo","aceites","tuberculo","verduras","frutas","azucar","cafe","especias",
         "otros_alim","bebidas","ali_fuera","tabaco","vesti_calz","vestido","calzado","vivienda",
         "alquiler","pred_cons","agua","energia","limpieza","cuidados","utensilios","enseres","salud",
         "atenc_ambu","hospital","medicinas","transporte","publico","foraneo","adqui_vehi","mantenim",
         "refaccion","combus","comunica","educa_espa","educacion","esparci","paq_turist","personales",
         "cuida_pers","acces_pers","otros_gas","transf_gas","percep_tot","retiro_inv","prestamos",
         "otras_perc","ero_nm_viv","ero_nm_hog","erogac_tot","cuota_viv","mater_serv","material",
         "servicio","deposito","prest_terc","pago_tarje","deudas","balance","otras_erog","smg"]

def read_file(year):
    path = 'http://www.beta.inegi.org.mx/contenidos/proyectos/enchogares/regulares/enigh/nc/'
    renames = {}
    pdb.set_trace()
    if year == '2016':
        name_file = '2016/microdatos/enigh2016_ns_concentradohogar_csv.zip'
        name_csv = 'concentradohogar.csv'
        renames = {'factor':'factor_hog'}
    elif year == '2014':
        name_file = '2014/microdatos/NCV_Concentrado_2014_concil_2010_csv.zip'
        name_csv = 'ncv_concentrado_2014_concil_2010.csv'
    elif year == '2012':
        name_file = '2012/microdatos/NCV_Concentrado_2012_concil_2010_csv.zip'
        name_csv = 'ncv_concentrado_2012_concil_2010_csv.csv'
    else:
        print('No information for that year')
        sys.exit()

    resp = urlopen(path + name_file)
    with ZipFile(BytesIO(resp.read())) as z:
        with z.open(name_csv) as f:
            df = pd.read_csv(f)

    if renames:
        df.rename(columns=renames, inplace=True)
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
