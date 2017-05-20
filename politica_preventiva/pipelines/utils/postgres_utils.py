#!/usr/bin/env python
# -*- coding: utf-8 -*-
import os
import sys
import json
import glob
import tempfile
import zipfile
import datetime
import shutil
#import rarfile
import requests
from requests.auth import HTTPDigestAuth
import logging
import logging.config
import boto
import numpy as np
import pandas as pd
from itertools import product
from bs4 import BeautifulSoup
from ftplib import FTP
import psycopg2
from dotenv import load_dotenv,find_dotenv

def connect_to_database():

    """ returns a connection to the database
    :param None
        Assumes that the dotenv file is discoverable with:
            POSTGRES_USER=""
            POSTGRES_PASSWORD=""
            PGPORT=5432
            PGHOST=""
            PGDATABASE=""

    :return [connection]
    """

    load_dotenv(find_dotenv())

    conn = psycopg2.connect(dbname=dbconf["PGDATABASE"],
                            host=dbconf["PGHOST"],
                            port=8427,
                            user=dbconf["PGUSER"],
                            password=dbconf["PGPASSWORD"])

    '''
    conn = psycopg2.connect(dbname = dbconf["PGDATABASE"],
                            host = tnconf["CONNECTION_HOST"],
                            port = tnconf["CONNECTION_PORT"],
                            user = dbconf["PGUSER"],
                            password = dbconf["PGPASSWORD"])
    '''
    conn.autocommit = True
    return conn
