#!/usr/bin/env python
# -*- coding: utf-8 -*-

""" Common string functions
"""

import pandas as pd
import numpy as np
import re


def remove_accent(x):

    subs = [('á', 'a'),
       ('é', 'e'),
       ('í', 'i'),
       ('ó', 'o'),
       ('ú', 'u'),
       ('Á', 'A'),
       ('É', 'E'),
       ('Í', 'I'),
       ('Ó', 'O'),
       ('Ú', 'U'),
       ('ñ', 'n'),
       ('Ñ', 'n'),
       ('¥', 'n')]

    if pd.isnull(x):
        return None

    if isinstance(x, str):
        x_2 = x

        for tup_subs in subs:
            x_2 = re.sub(tup_subs[0], tup_subs[1], x_2)
        return x_2

    else:

        return None

def str_to_int(x, missing='zero'):

    if isinstance(x, str):
        x = re.sub(',','',x)

    try:
        return int(x)

    except ValueError:

        if missing == 'zero':
            return 0
        else:
            return None

def str_to_float(x, missing='zero'):

    if isinstance(x, str):
        x = re.sub(',','',x)

    try:
        return int(x)

    except ValueError:

        if missing == 'zero':
            return 0

        else:
            return None
