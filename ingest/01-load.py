# coding: utf-8
""" Check and download most resent raw data for the proyect política preventiva
and upload it in an AWS s3 bucket.  [run this script from /ingest]

This is a script that downloads all the non sedesol databases from internet and
ftp sources.


CENAPRED

INEGI -INPC

"""

from utilsAPI import *
#import psycopg2

#con = psycopg2.connect(dbname= conf["PGDATABASE"], host=conf["PGHOST"], 
#	port= conf["PGPORT"], user= conf["PGUSER"], password= conf["PGPASSWORD"])


# Get Riesgos from CENAPRED
cenapred = get_cenapred_data()


# Get Precios from inegi
INPC, metadata = get_inpc_ciudad_data()

#Download MSM shapefiles into s3bucket-local?
get_smn_data(year='2016', location="local")
