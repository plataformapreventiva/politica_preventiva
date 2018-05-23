#!/usr/bin/env python

from sqlalchemy import create_engine
import dotenv
import io
import numpy as np
import pandas as pd
import os
import psycopg2

env_file_path = '/home/monizamudio/workspace/politica_preventiva/politica_preventiva/.env'

dotenv.load_dotenv(env_file_path)
pg_user = os.getenv('POSTGRES_USER')
pg_password = os.getenv('POSTGRES_PASSWORD')
pg_host = os.getenv('PGHOST')
pg_port = os.getenv('PGPORT')
pg_db = os.getenv('PGDATABASE')
prod_pg_password = os.getenv('PROD_POSTGRES_PASSWORD')
prod_pg_host = os.getenv('PROD_PGHOST')

# Development database
pg_url = 'postgresql://{}:{}@{}:{}/{}'.format(pg_user, pg_password, pg_host, pg_port, pg_db)
engine = create_engine(pg_url)

pipeline_task = 'cuaps_criterios'
sql_statement = 'SELECT * from raw.{}_dic'.format(pipeline_task)

try:
    dic = pd.read_sql_query(sql_statement, con = engine)
except Exception as e:
    print(e)

# Save to csv
output_path = '/data/dictionaries/{}_dic.csv'.format(pipeline_task)
dic.to_csv(output_path, sep = '|', index = False)
