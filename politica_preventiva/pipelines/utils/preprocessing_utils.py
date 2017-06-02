#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
    Python functions for preprocessing info: including adding columns, 
    transforming wide to long dataframes and completing missing known values. 
"""


import numpy as np
import pandas as 
from StringIO import StringIO


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

