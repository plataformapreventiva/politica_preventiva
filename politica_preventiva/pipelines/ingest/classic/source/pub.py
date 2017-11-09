# -*- coding: utf-8 -*-
from __future__ import print_function
import argparse
import boto3
import re
import datetime
import os

from pyspark.sql import SQLContext
from pyspark import SparkContext
from pyspark.sql import functions as sf
from pyspark.sql.functions import to_date, lit
from functools import reduce
from itertools import product

os.environ["PYSPARK_PYTHON"]="/usr/local/bin/python3"

def trim(string):
    try:
        return string.strip()
    except:
        return ""


def clean_column_names(x):
    x = x.strip()
    x = x.lower()
    x = re.sub('\\x00', '', x)
    x = re.sub('ubuntu', '', x)
    x = re.sub('\\x80\\x03', '', x)
    x = x.strip()
    return x


def read_pub(spark, year, input_path, output_path):
    df = spark.read.csv(input_path + "pub_{0}.tar.gz".format(year), sep="|",header=True, encoding="latin1")
    cleaner = lambda x: clean_column_names(x)
    oldColumns = list(df.schema.names)
    newColumns = list(map(cleaner, oldColumns))
    newColumns[0] = "cd_dependencia"
    df = reduce(lambda df, idx: df.withColumnRenamed(oldColumns[idx], newColumns[idx]), range(len(oldColumns)), df)
    df.registerTempTable('table')
    return df


def clean_pub(spark):
    # clean age
    df = spark.sql("""
                  SELECT *, TO_DATE(CAST(UNIX_TIMESTAMP(FH_NACIMIENTO, 'yyyyMMdd')
                  AS TIMESTAMP)) AS birthdate FROM table""")
    df = df.withColumn("age", (sf.datediff(to_date(lit(datetime.date.today())), sf.col("birthdate")))/365)
    # create age categories
    categoria = sf.when(sf.col("age") >= 60, "Adulto Mayor").when((sf.col("age") <= 14) &
      (sf.col("age") >= 0), "Infante").when((sf.col("age") > 14) & (sf.col("age") < 60),
      "Adulto").otherwise(None)
    df = df.withColumn("categoria_edad", categoria)

    # clean muni
    df = df.withColumn("cve_muni", sf.concat(sf.col('cve_ent'), sf.lit(''), sf.col('cve_mun')))

    # clean sexo
    df.registerTempTable('table')
    df = spark.sql("""
                   select *,
                          (case when (cd_sexo like '%H%') then 'H'else
                                  case when (cd_sexo like '%M%') then 'M' else null end
                                  end) as categoria_sexo
                   from table
                   """)
    df.registerTempTable("pub")


def pub_agrupaciones(spark, output_path, agg_dict, tipo):
    """
    """
    agrupaciones = [x for x in agg_dict.keys() if agg_dict[x] == True]
    if len(agrupaciones) > 0:
        agrupaciones = ", " + ", ".join(agrupaciones)
    else:
        agrupaciones = ""
    wheres = " ".join(["and {x} is not null".format(x=x)
                       for x in agg_dict.keys() if agg_dict[x] == True])
    selects = {x: ("cast('' as varchar(3))" if not agg_dict[x]
                   else x) for x in agg_dict.keys()}
    # Nivel
    if not agg_dict['cve_ent']:
        nivel = 'nacional'
    elif agg_dict['cve_muni']:
        nivel = 'municipal'
    else:
        nivel = 'estatal'
    # Grupo
    if agg_dict['cd_padron']:
        grupo = 'padron'
    else:
        grupo = 'programa'
    if tipo == 'total':
        select_tipo = "cast('total' as varchar(20))"
    elif tipo == 'sexo':
        select_tipo = 'categoria_sexo'
        wheres = wheres + ' and categoria_sexo is not null'
        agrupaciones = ", categoria_sexo" + agrupaciones
    else:
        select_tipo = 'categoria_edad'
        wheres = wheres + ' and categoria_edad is not null'
        agrupaciones = ", categoria_edad" + agrupaciones

    query = ("""select anio,
                        {ent} AS cve_ent,
                        {muni} AS cve_muni,
                        cd_programa,
                        {padron} AS cd_padron,
                        count(distinct new_id) as n_beneficiarios,
                        sum(nu_imp_monetario) as suma_importe,
                        {tipo} as tipo,
                        cast('{nivel}' as varchar(20)) as nivel,
                        cast('{grupo}' as varchar(20)) as grupo
                        from pub
                    where cd_programa is not null
                    {wheres}
                    group by anio, cd_programa{groupbys}
            """.format(ent=selects['cve_ent'],
                       muni=selects['cve_muni'],
                       padron=selects['cd_padron'],
                       nivel=nivel,
                       grupo=grupo,
                       groupbys=agrupaciones,
                       wheres=wheres,
                       tipo=select_tipo))
    print(query)

    spark.sql(query).write.csv(output_path, sep='|', header=True, mode='append')
    return True


if __name__ == "__main__":
    input_path = "s3n://pub-raw/"
    sc = SparkContext.getOrCreate()
    spark = SQLContext(sc)
    parser = argparse.ArgumentParser(description="S3 file combiner")
    parser.add_argument("--year", help="year", default = "2013") 
    args = parser.parse_args()
    year = args.year
    print(year)
    output_path = 's3n://dpa-plataforma-preventiva/etl/pub/preprocess/{}'.format(year)
    print(output_path)
    df = read_pub(spark, year, input_path, output_path)
    clean_pub(spark)
    # Generate levels of aggregation
    niveles = ['cve_ent', 'cve_muni', 'cd_padron']
    niveles_dicts = [{niveles[0]:i, niveles[1]:j, niveles[2]:k} 
            for i, j, k in product([True, False],
                        [True, False],
                        [True, False])]

    niveles_dicts = [xi for xi in niveles_dicts
            if not (xi['cve_ent'] == False and
                xi['cve_muni'] == True)]

    tipos = ['total', 'sexo', 'edad']

    #loop through all aggregations
    exito = []
    for agg_dict in niveles_dicts:
        print(agg_dict)
        for tipo in tipos:
          print('\t' + tipo)
          exito.append(pub_agrupaciones(spark, output_path, agg_dict, tipo))

    if(all(exito)):
       f = open("exitoso.txt","w+")
       f.write("exito")
       s3 = boto3.resource('s3')
       out = 'etl/pub/preprocess/{}/exitoso.txt'.format(year)
       s3.Object('dpa-plataforma-preventiva', out).put(Body=open('exitoso.txt', 'rb'))
