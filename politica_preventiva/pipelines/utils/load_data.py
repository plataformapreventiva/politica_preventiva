#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
Tasks for loading data from CSVs in S3 to a database
Our pipeline generates features locally using pandas. Using these tasks, it
loads the results to the sedesol postgres database.
"""

import logging
import logging.config
import json
from collections import OrderedDict

import luigi
from luigi import configuration

import utils.pg_sedesol as pg_sed
from utils.get_features import GetFeatures
from utils.get_responses import GetResponses

LOGGING_CONF = configuration.get_config().get("core", "LOGGING_CONF_file")
logging.config.fileConfig(LOGGING_CONF)
logger = logging.getLogger("sedesol.pipeline")



s3toclean
    # sedesol login options
    conf = configuration.get_config()
    with open(conf.get('postgres', 'db_profile')) as json_file:
        db_profile = json.load(json_file)

    port = db_profile["PGPORT"]
    host = db_profile["PGHOST"]
    database = db_profile["PGDATABASE"]
    user = db_profile["PGUSER"]
    password = db_profile["PGPASSWORD"]

    

