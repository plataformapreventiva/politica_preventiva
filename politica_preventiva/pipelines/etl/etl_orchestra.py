# coding: utf-8

import re
import os
import ast
import luigi
import psycopg2
import boto3
import random
import sqlalchemy
import tempfile
import glob
import datetime
import subprocess
import pandas as pn
from luigi import six
from os.path import join, dirname
from luigi import configuration
from luigi.contrib import postgres
from luigi.s3 import S3Target, S3Client
from dotenv import load_dotenv,find_dotenv
from luigi.contrib.postgres import PostgresTarget, PostgresQuery

from utils.pg_sedesol import parse_cfg_string, download_dir

# Variables de ambiente
load_dotenv(find_dotenv())

# Load Postgres Schemas
#temp = open('./common/pg_clean_schemas.txt').read()
#schemas = ast.literal_eval(temp)

# AWS
aws_access_key_id = os.environ.get('AWS_ACCESS_KEY_ID')
aws_secret_access_key = os.environ.get('AWS_SECRET_ACCESS_KEY')

class ETL(luigi.Task):

    year_month = luigi.Parameter()

    def requires(self):

    	return MergeDBs(self.year_month)

class CreateSemanticDB(postgres.PostgresQuery):

	year_month = luigi.Parameter()
	sql_scripts = luigi.Parameter('EtlPipeline')
	database = os.environ.get("PGDATABASE_COMPRANET")
	user = os.environ.get("POSTGRES_USER_COMPRANET")
	password = os.environ.get("POSTGRES_PASSWORD_COMPRANET")
	host = os.environ.get("PGHOST_COMPRANET")

	def requires(self):
		return CreateCleanDB(self.year_month)

	@property
	def update_id(self):
		num = str(random.randint(0,100000))
		return num 

	@property
	def table(self):
		return "clean.funcionarios" 

	@property
	def query(self):
		sqlfile = open('./etl/sql_scripts/merge.sql', 'r')
		query = sqlfile.read()
		return query 

	def output(self):
		
		return luigi.postgres.PostgresTarget(host=self.host,database=self.database,user=self.user,
			password=self.password,table=self.table,update_id=self.update_id)

class CreateCleanDB(postgres.PostgresQuery):

	"""
	
	"""

	year_month = luigi.Parameter()
	sql_scripts = luigi.Parameter('EtlPipeline')
	database = os.environ.get("PGDATABASE_COMPRANET")
	user = os.environ.get("POSTGRES_USER_COMPRANET")
	password = os.environ.get("POSTGRES_PASSWORD_COMPRANET")
	host = os.environ.get("PGHOST_COMPRANET")

	@property
	def update_id(self):
		num = str(random.randint(0,100000))
		return num 

	@property
	def table(self):
		return "clean.diccionario_dependencias" 

	@property
	def query(self):
		sqlfile = open('./etl/sql_scripts/clean.sql', 'r')
		query = sqlfile.read()
		return query 

	def output(self):
		
		return luigi.postgres.PostgresTarget(host=self.host,database=self.database,user=self.user,
			password=self.password,table=self.table,update_id=self.update_id)
