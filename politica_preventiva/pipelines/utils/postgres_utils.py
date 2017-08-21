#!/usr/bin/env python
# -*- coding: utf-8 -*-

""" PostGres Utils 

This module contains utilities for PostGres

    Assumes that these environmental variables are discoverable:
        POSTGRES_USER=""
        POSTGRES_PASSWORD=""
        PGPORT=5432
        PGHOST=""
        PGDATABASE=""

"""

import os
import psycopg2
from sqlalchemy import create_engine
from dotenv import load_dotenv,find_dotenv
from dotenv import load_dotenv,find_dotenv
load_dotenv(find_dotenv())


def connect_to_db():

    """ returns a connection to the database
    :param None

    :return [connection]
    """
    print(os.environ.get("PGDATABASE"))
    print(os.environ.get("POSTGRES_USER"))
    print(os.environ.get("PGHOST"))
    print(os.environ.get("POSTGRES_PASSWORD"))

    conn = psycopg2.connect(dbname=os.environ.get("PGDATABASE"),
                            user = os.environ.get("POSTGRES_USER"),
                            host = os.environ.get("PGHOST"),
                            password = os.environ.get("POSTGRES_PASSWORD"),
                            port=5432)
    conn.autocommit = True
    return conn

from sqlalchemy import create_engine
from sqlalchemy.pool import NullPool


def get_engine():
    """
    Get SQLalchemy engine using credentials.
    """

    url = 'postgresql://{user}:{passwd}@{host}:{port}/{db}'.format(
                                                            user=os.environ.get("POSTGRES_USER"),
                                                            passwd=os.environ.get("POSTGRES_PASSWORD"),
                                                            host=os.environ.get("PGHOST"),
                                                            port=5432,
                                                            db=os.environ.get("PGDATABASE"))
    engine = create_engine(url, echo='debug')
    return engine
