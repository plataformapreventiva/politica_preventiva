#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
	Preprocessing Functions
"""
import boto3 
import pdb
from luigi import configuration
import pandas as pd
from io import StringIO
import politica_preventiva.pipelines.utils.preprocessing_utils as pputils
from politica_preventiva.pipelines.utils.pipeline_utils import s3_to_pandas, get_extra_str, pandas_to_s3, copy_s3_files, delete_s3_file 


def precios_granos_prep(year_month, s3_file, extra_h, out_key):
    """
    Preprocessing function for precios_granos: reads df from s3, completes
    missing values, turns wide-format df to a long-format df, and uploads to s3
    """
    bucket = 'dpa-plataforma-preventiva'
    df = pputils.check_empty_dataframe(bucket,'etl/' + s3_file, out_key)

    if df is not None:
        df['producto'] = pputils.complete_missing_values(df['producto'])
        columns = ['sem_1', 'sem_2', 'sem_3', 'sem_4', 'sem_5', 'prom_mes']
        df = pputils.gather(df, 'semana', 'precio', columns)
        df['semana'] = df['semana'].map(lambda x: x.replace('sem_', ''))
        df = df[df['semana'] != 'prom_mes'] 
        df.loc[df.precio == '--', 'precio'] = None
        pandas_to_s3(df, 'dpa-plataforma-preventiva', out_key)

    return True


def sagarpa_prep(year_month, s3_file, extra_h, out_key):
    """
    Preprocessing function for sagarpa: reads df from s3, completes missing values, 
    turns wide-format df to a long-format df, and uploads to s3
    """
    bucket = 'dpa-plataforma-preventiva'
    df = pputils.check_empty_dataframe(bucket,'etl/' + s3_file, out_key)


    if df is not None:
        df['estado'] = pputils.complete_missing_values(df['estado'])
        df['distrito'] = pputils.complete_missing_values(df['distrito'])
        df['cultivo'] = extra_h 

        pandas_to_s3(df, 'dpa-plataforma-preventiva', out_key)
    return True

def ipc_ciudades_prep(year_month, s3_file, extra_h, out_key):
    """
    Preprocessing function for inpc: reads df from s3, parses dates 
    and uploads to s3. 
    """   
    bucket = 'dpa-plataforma-preventiva'
    df = pputils.check_empty_dataframe(bucket,'etl/' + s3_file, out_key)


    if df is not None:
        df['month'] = df['fecha'].map(lambda x: pputils.inpc_month(x))
        df['year'] = df['fecha'].map(lambda x: pputils.inpc_year(x))
        df = pputils.replace_missing_with_none(df) 
        pandas_to_s3(df, 'dpa-plataforma-preventiva', out_key)
    return True

def indesol_prep(year_month, s3_file, extra_h, out_key):
    """
    Preprocessing function for indesol: reads df from s3, turns wide-format df to long-format, 
    turns columns to json and uploads to s3. 
    """
    bucket = 'dpa-plataforma-preventiva'
    df = pputils.check_empty_dataframe(bucket,'etl/' + s3_file, out_key)
    
    if df is not None:
    # Change Actividad columns from wide to long format
        columns = ['ACTIVIDAD_' + str(x) for x in range(1,20)]
        df = pputils.gather(df, 'ACTIVIDAD', 'EDO_ACTIVIDAD', columns)
        df['ACTIVIDAD'] = df['ACTIVIDAD'].map(lambda x: x.replace('ACTIVIDAD_', ''))

        # Rename some long columns
        # TODO: CHANCE THIS TO REGEX
        columns = [x for x in df.columns if 'INFORME' in x]
        informe_dict = {col: col.replace('INFORME ', '') for col in columns}
        informe_dict = {key:informe_dict[key].replace(' EN TIEMPO', 'T') for\
                key in informe_dict.keys()}
        informe_dict = {key:informe_dict[key].replace(' PRESENTADO', 'P') for\
                key in informe_dict.keys()}
        df = df.rename(columns=informe_dict)    

        # Turn Informe columns into json column 
        columns = list(informe_dict.values())
        df = pputils.df_columns_to_json(df, columns, 'INFORMES')

        pandas_to_s3(df, 'dpa-plataforma-preventiva', out_key)
    return True

def sagarpa_cierre_prep(year_month, s3_file, extra_h, out_key):
    # TODO: ver si es menor a 2013 (bajado como tabla completa, o mayor, 
    # y homologar columnas de ambos casos)
    bucket = 'dpa-plataforma-preventiva'
    pputils.no_preprocess_method(bucket, 'etl/' + s3_file, out_key)
    return True

def no_preprocess_method(year_month, s3_file, extra_h, out_key):
   bucket = 'dpa-plataforma-preventiva'
   pputils.no_preprocess_method(bucket, 'etl/' + s3_file, out_key)
   return True
