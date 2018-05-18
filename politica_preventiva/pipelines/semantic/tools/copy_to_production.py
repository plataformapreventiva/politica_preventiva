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

# Production database
prod_pg_url = 'postgresql://{}:{}@{}:{}/{}'.format(pg_user, prod_pg_password, prod_pg_host, pg_port, pg_db)
prod_engine = create_engine(prod_pg_url)

sql_statement = 'SELECT actualizacion_sedesol, data_date, plot, variable, valor AS categoria, label, count AS valor FROM semantic.reporte_programas;'

try:
    program_report = pd.read_sql_query(sql_statement, con = engine)
except Exception as e:
    print(e)

program_report['label'] = program_report['label'].replace('', program_report['categoria'])
program_report['actualizacion_sedesol'] = program_report['actualizacion_sedesol'].apply(lambda x: pd.to_datetime(str(x)))

program_report.to_sql('semantic.reporte_programas', engine, if_exists = 'replace')
program_report.to_sql('semantic.reporte_programas', prod_engine, if_exists = 'replace')


