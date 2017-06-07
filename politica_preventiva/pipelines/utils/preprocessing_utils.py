#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
    Python functions for preprocessing info: including adding columns, 
    transforming wide to long dataframes and completing missing known values. 
"""


import numpy as np
import pandas as pd

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
    df_json = df_json.split('},{')
    df_json = [clean_json_strings(x) for x in df_json]
    df[new_name] = df_json

    return df
    
def clean_json_strings(x):
    """
    Simple function to clean strings used in df_columns_to_json
    """
    if x[0] == '[':
        x = x.replace('[', '')
    if x[0] != '{':
        x = '{' + x
    if x[-1] != '}':
        x = x + '}'
    return x