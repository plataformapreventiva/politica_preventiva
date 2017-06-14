#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
Ingesta SEGOB Secretariado Ejecutivo
	http://www.banxico.org.mx/consultas-atm/cajeros.json
"""

from BeautifulSoup import BeautifulSoup, SoupStrainer
import requests
import re
import pycurl
from StringIO import StringIO
import ntpath
import hashlib
import urllib
import os
import lib.victimas_comun as v
from lib.clean_states import CrimeMunicipios, CrimeStates
import pandas as pd
import numpy as np
import pandas.io.sql as pd_sql
import sqlite3 as sq
import zipfile
import re
