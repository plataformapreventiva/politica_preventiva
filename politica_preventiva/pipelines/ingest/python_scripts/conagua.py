#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""Utilidades Conagua

Funciones de Descarga y limpieza de Task Conagua

TODO()
    #Load all files into folder
    #it should check if already exists

"""

import os
import requests
import numpy as np
import pandas as pd
import json
from requests.auth import HTTPDigestAuth
import datetime
from itertools import product
from bs4 import BeautifulSoup
from ftplib import FTP
import requests

def conagua_smn(year='2016',location="s3"):

    """Downloads shp files from CONAGUA Monitor de Sequía de Mexico (smn) into
        specified location

    Args:
        year (str): give year wanted

    Returns:
        dataframe: Saves a DataFrame with all Bank cashier information from Banxico.


    """

    ftp = FTP('200.4.8.36')     # connect to host, default port
    ftp.login(conf["SMN_USER"],conf["SMN_PASSWORD"])
    ftp.cwd(year)
    filenames = ftp.nlst()

    for filename in filenames:
        local_filename = os.path.join('../data/SMN', filename)
        file = open(local_filename, 'wb')
        ftp.retrbinary('RETR '+ filename, file.write)

    ftp.quit()