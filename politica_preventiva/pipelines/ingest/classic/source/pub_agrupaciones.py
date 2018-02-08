from __future__ import print_function

import argparse
import boto3
import os
import yaml
import pyspark.sql.functions as func

from pyspark import SparkContext
from pyspark.sql import SQLContext
from pyspark.sql.types import *
from itertools import product

# create spark context and SQL context
sc = SparkContext.getOrCreate()
sqlContext = SQLContext(sc)


SCHEMA = ['anio',
          'cve_ent',
          'cve_muni',
          'cve_programa',
          'cve_padron',
          'tipo',
          'temporalidad',
          'mes',
          'nivel',
          'grupo',
          'n_beneficiarios',
          'suma_importe']

with open('/home/hadoop/pub_agg.yaml', 'r') as f:
    D = yaml.load(f)


def read_pub(input_path, year):
    # define schema
    customSchema = StructType([
        StructField("cd_dependencia", StringType(), True),
        StructField("origin", StringType(), True),
        StructField("id_amin", StringType(), True),
        StructField("cd_programa", StringType(), True),
        StructField("cd_padron", StringType(), True),
        StructField("anio", IntegerType(), True),
        StructField("periodo", StringType(), True),
        StructField("nb_primer_ap", StringType(), True),
        StructField("nb_segundo_ap", StringType(), True),
        StructField("nb_nombre", StringType(), True),
        StructField("fh_nacimiento", StringType(), True),
        StructField("edad", IntegerType(), True),
        StructField("categoria_edad", StringType(), True),
        StructField("cd_sexo", StringType(), True),
        StructField("nu_mes_pago", IntegerType(), True),
        StructField("nu_mes_correspondencia", IntegerType(), True),
        StructField("new_id", StringType(), True),
        StructField("nu_imp_monetario", IntegerType(), True),
        StructField("cve_muni", StringType(), True),
        StructField("cve_ent", StringType(), True),
        StructField("cve_loc", StringType(), True),
        StructField("in_titular", StringType(), True),
        StructField("cd_beneficio", StringType(), True)])

    filename = '{path}/pub_{year}.csv'.format(path=input_path,
                                                     year=year)
    df = sqlContext.read \
                   .format('com.databricks.spark.csv') \
                   .option("header", 'false') \
                   .option("delimiter", "|") \
                   .load(filename, schema = customSchema)
    return df


def pub_aggregation(input_path,
                    year,
                    output_path,
                    nivel,
                    tipo,
                    grupo,
                    temporalidad):
    # Read pub
    df = read_pub(input_path, year)
    

    groups = D['niveles'][nivel]['key_col'] + \
             D['grupos'][grupo]['key_col'] + \
             D['tipos'][tipo]['key_col'] + \
             D['temporalidad'][temporalidad]['key_col']

    new_names = D['niveles'][nivel]['new_names'] + \
                D['grupos'][grupo]['new_names'] + \
                D['tipos'][tipo]['new_names'] + \
                D['temporalidad'][temporalidad]['new_names']

    add_columns = D['niveles'][nivel]['add_column'] + \
                  D['grupos'][grupo]['add_column'] + \
                  D['temporalidad'][temporalidad]['add_column']

    # drop null
    df = df.na.drop(subset=groups)
    print('------------------')
    print('groups : {}'.format(groups))
    print('cols: {}'.format(add_columns))

    agg = [func.countDistinct('new_id').alias('n_beneficiarios'),
           func.sum('nu_imp_monetario').alias('suma_importe')]
    df_agg = df.groupBy(*groups).agg(*agg)

    # Change column names
    new_cols = new_names + ['n_beneficiarios', 'suma_importe']
    df_agg = df_agg.toDF(*new_cols)

    # Add new empty columns
    for col in add_columns:
        df_agg = df_agg.withColumn(col, func.lit(None).cast(StringType()))

    # Extra columns
    df_agg = df_agg.withColumn('grupo', func.lit(grupo))
    df_agg = df_agg.withColumn('nivel', func.lit(nivel))
    df_agg = df_agg.withColumn('temporalidad', func.lit(temporalidad))

    if tipo == 'total':
        df_agg = df_agg.withColumn('tipo', func.lit(tipo))

    # Store it
    df_agg.select(SCHEMA).write.csv(output_path, sep='|', header=False, mode='append')


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="S3 file combiner")
    parser.add_argument("--year", help="year", default = "2013")
    args = parser.parse_args()
    year = args.year
    print(year)
    output_path = 's3n://dpa-plataforma-preventiva/etl/pub_agrupaciones/preprocess/{}'.format(year)
    input_path = "s3n://pub-raw/concatenation/{}".format(year)

    # Read yaml
    niveles = D['niveles'].keys()
    grupos = D['grupos'].keys()
    tipos = D['tipos'].keys()
    temporalidad = D['temporalidad'].keys()
    for nivel, grupo, tipo, temp in product(niveles, grupos, tipos, temporalidad):
        pub_aggregation(input_path,
                    year,
                    output_path,
                    nivel,
                    tipo,
                    grupo,
                    temp)
    f = open("exitoso.txt", "w+")
    s3 = boto3.resource('s3')
    out = 'etl/pub_agrupaciones/preprocess/{}/exitoso.txt'.format(year)
    s3.Object('dpa-plataforma-preventiva', out).put(Body=open('exitoso.txt', 'rb'))
    f.close()
