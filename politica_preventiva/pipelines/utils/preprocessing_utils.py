#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
    Python functions for preprocessing info: including adding columns, 
    transforming wide to long dataframes and completing missing known values. 
"""


import numpy as np
import pandas as pd
import json
import ast
import os
import boto3
from politica_preventiva.pipelines.utils.pipeline_utils import s3_to_pandas, copy_s3_files, delete_s3_file, get_s3_file_size
from dotenv import load_dotenv, find_dotenv


def gather(df, key, value, cols):
    """
    Function similar to tidyr gather; takes a wide df and turns it into a long df.
    Args;
        (df): wide data frame to transform
        (key): string name for the variable that contains column names
        (value): string name for the variable that will contain the values
        (cols): list/dict of columns with to change into one variable
    """
    id_vars = [col for col in df.columns if col not in cols]
    id_values = cols
    var_name = key
    value_name = value
    return pd.melt(df, id_vars, id_values, var_name, value_name)

def complete_missing_values(col):
    """
    Function for filling in values that are known but missing explicitly; eg, in 
    precios_granos if a 'producto' contains many 'origenes', the product is only 
    specifiend in the first row. 
    Args:
        (col): column to complete    
    """
    col_comp = []
    last = col[0]
    for row in col:
        if not is_empty(row):
            last = row
            col_comp.append(row)
        else:
            col_comp.append(last)
    return col_comp


def is_empty(x):
    """
    Simple checking if something is either None, an empty string or nan
    """
    if not x:
        return True
    try:
        if np.isnan(x):
            return True
    except (TypeError, AttributeError):
        pass
    try:
        if pd.isnull(x):
            return True
    except Exception:
        pass

    return False


def inpc_month(x):
    """
    Preprocessing INPC Month: get month from fecha
    """
    dict_month = {'Ene':'01', 'Feb':'02', 'Mar':'03', 'Abr':'04',
    'May': '05', 'Jun':'06', 'Jul':'07', 'Ago': '08', 'Sep':'9',
    'Oct': '10', 'Nov':'11', 'Dic':'12'}
    try:
        x2 = x.split(' ')
        return dict_month[x2[0]]
    except (KeyError, AttributeError):
        return None


def inpc_year(x):
    """
    Preprocessing INPC: Get year from fecha
    """
    try:
        x2 = x.split(' ')
        return x2[1]
    except KeyError:
        return None

def df_columns_to_json(df, columns, new_name):
    """
    Removes *columns* from *df*, turns them into a json string, and 
    add it to *df* in a column called *new_name*
    """
    # Convert extra columns to json string
    df_json = df[columns].to_json(orient='records')

    # Drop extra columns
    df = df.drop(columns, axis=1)

    # Add column with extra value 
    df_json = [json.dumps(x) for x in ast.literal_eval(df_json)]
    df[new_name] = df_json

    return df

def check_empty_dataframe(bucket, s3_file, out_key):
    """
    Tries to read a dataframe. If the file is empty, it copies the emtpy file
    to the next stage, but erases the original file. Otherwise it returns the 
    original dataframe
    """
    try:
        df = s3_to_pandas(Bucket=bucket, Key=s3_file)

    except Exception: # TODO: Change to real error, EmptyDataError
        copy_s3_files(input_bucket=bucket, input_key=s3_file, 
            output_bucket=bucket, output_key=out_key)    
        delete_s3_file(Bucket=bucket, Key=s3_file)
        #write_missing_csv()
        df = None
    return df

def no_preprocess_method(bucket, s3_file, out_key):
    """
    Method for files that don't really require preprocessing.
    Checks if a file is empty without loading it. If it is smaller than 
    10 b, the file is assumed to be empty. 
    """
    file_size = get_s3_file_size(Bucket=bucket, Key=s3_file)
    copy_s3_files(input_bucket=bucket, input_key=s3_file, 
        output_bucket=bucket, output_key=out_key)
    if file_size < 10:
        delete_s3_file(bucket, s3_file)

def replace_missing_with_none(df):
    """
    Method to replace all types of null to None
    """
    str_replace = r'N/E'
    df.replace({str_replace:None}, regex=True, inplace=True)

    # replace NAN values
    df = df.where((pd.notnull(df)), None)
    return df






