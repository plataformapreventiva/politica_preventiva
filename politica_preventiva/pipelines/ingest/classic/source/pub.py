# -*- coding: utf-8 -*-
from __future__ import print_function
import argparse
# import boto3
import re
import datetime
import os
from pyspark.sql import SQLContext
from pyspark import SparkContext
from pyspark.sql import functions as sf
from pyspark.sql.functions import to_date, lit
from functools import reduce
from itertools import product

os.environ["SPARK_HOME"] = "/usr/local/Cellar/apache-spark/1.5.1/"
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
    df = spark.read.csv(input_path + "pub_{0}.tar.gz".format(year),
                        sep="|", header=True, encoding="latin1")
    df.registerTempTable('table')
    cleaner = lambda x: clean_column_names(x)
    oldColumns = list(df.schema.names)
    newColumns = list(map(cleaner, oldColumns))
    newColumns[0] = "cd_dependencia"
    df = reduce(lambda df, idx: df.withColumnRenamed(oldColumns[idx],
                                                     newColumns[idx]),
                range(len(oldColumns)), df)
    return df


def clean_pub(spark, df):
    # clean age
    df = spark.sql("""
                  SELECT *,
                   TO_DATE(CAST(UNIX_TIMESTAMP(FH_NACIMIENTO, 'yyyyMMdd')
                  AS TIMESTAMP)) AS birthdate FROM table""")
    df = df.withColumn("age",
                       (sf.datediff(to_date(lit(datetime.date.today())),
                                    sf.col("birthdate")))/365)
    # create age categories
    categoria = sf.when(sf.col("age") >= 60, "Adulto Mayor").when((sf.col("age") <= 14) &
      (sf.col("age") >= 0), "Infante").when((sf.col("age") > 14) & (sf.col("age") < 60),
      "Adulto").otherwise(None)
    df = df.withColumn("categoria_edad", categoria)
    # clean muni
    df = df.withColumn("cve_muni", sf.concat(sf.col('cve_ent'), sf.lit(''), sf.col('cve_mun')))
    # clean sexo
    categoria_sexo = sf.when((sf.col("cd_sexo") != 'H') & (sf.col("cd_sexo") != 'M'), None)
    df = df.withColumn("cd_sexo", categoria_sexo)
    df.registerTempTable("pub")
    return df


def pub_agrupaciones(spark, df, output_path, year, agg_dict):
    """
    """
    agrupaciones = [x for x in agg_dict.keys() if agg_dict[x] == True]
    if len(agrupaciones) > 0:
        agrupaciones = ", " + ",".join(agrupaciones)
    else:
        agrupaciones = ""
    wheres = " ".join(["AND {x} is not null".format(x=x)
                       for x in agg_dict.keys() if agg_dict[x]==True])
    selects = { x:("cast('' as varchar(3))" if not agg_dict[x]
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
    # Tipo: total
    query_total = (""" select anio,
                    {ent} AS cve_ent,
                    {muni} AS cve_muni,
                    cd_programa,
                    {padron} AS cd_padron,
                    count(distinct new_id) as n_beneficiarios,
                    cast('' as FLOAT) as suma_importe,
                    cast('total' as varchar(20)) as tipo,
                    cast('{nivel}' as varchar(20)) as nivel,
                    cast('{grupo}' as varchar(20)) as grupo
                    from pub
                    where cd_programa is not null
                    {wheres}
                    group by anio, cd_programa {groupbys}
            """.format(ent=selects['cve_ent'],
                       muni=selects['cve_muni'],
                       padron=selects['cd_padron'],
                       nivel=nivel,
                       grupo=grupo,
                       groupbys=agrupaciones,
                       wheres=wheres))
    nivel_total = spark.sql(query_total)
    # Tipo: sexo
    query_sexo = (""" select anio,
                    {ent} AS cve_ent,
                    {muni} AS cve_muni,
                    cd_programa,
                    {padron} AS cd_padron,
                    count(distinct new_id) as n_beneficiarios,
                    cast('' as FLOAT) as suma_importe,
                    cd_sexo as tipo,
                    cast('{nivel}' as varchar(20)) as nivel,
                    cast('{grupo}' as varchar(20)) as grupo
                    from pub
                    where cd_programa is not null
                    and cd_sexo is not null
                    {wheres}
                    group by anio, cd_programa, cd_sexo {groupbys}
            """.format(ent=selects['cve_ent'],
                       muni=selects['cve_muni'],
                       padron=selects['cd_padron'],
                       nivel=nivel,
                       grupo=grupo,
                       groupbys=agrupaciones,
                       wheres=wheres))
    nivel_sexo = spark.sql(query_sexo)
    # Tipo: categorias de edad
    query_edad = (""" select anio,
                    {ent} AS cve_ent,
                    {muni} AS cve_muni,
                    cd_programa,
                    {padron} AS cd_padron,
                    count(distinct new_id) as n_beneficiarios,
                    cast('' as FLOAT) as suma_importe,
                    categoria_edad as tipo,
                    cast('{nivel}' as varchar(20)) as nivel,
                    cast('{grupo}' as varchar(20)) as grupo
                    from pub
                    where cd_programa is not null
                    and categoria_edad is not null
                    {wheres}
                    group by anio, cd_programa, categoria_edad {groupbys}
            """.format(ent=selects['cve_ent'],
                       muni=selects['cve_muni'],
                       padron=selects['cd_padron'],
                       nivel=nivel,
                       grupo=grupo,
                       groupbys=agrupaciones,
                       wheres=wheres))
    nivel_edad = spark.sql(query_edad)
    nivel = nivel_total.unionAll(nivel_sexo.unionAll(nivel_edad))
    nivel.write.csv(output_path, sep='|', header=True, mode='append')
    return True


if __name__ == "__main__":
    input_path = "s3n://pub-raw/"
    sc = SparkContext.getOrCreate()
    spark = SQLContext(sc)
    parser = argparse.ArgumentParser(description="S3 file combiner")
    parser.add_argument("--year", help="year", default = "2014") 
    args = parser.parse_args()
    year = args.year
    print(year)
    output_path = 's3n://dpa-plataforma-preventiva/etl/pub/preprocess/{}'.format(year)
    print(output_path)
    df = read_pub(spark, year, input_path, output_path)
    df = clean_pub(spark, df)
    # Generate levels of aggregation
    niveles = ['cve_ent', 'cve_muni', 'cd_padron']
    niveles_dicts = [{niveles[0]:i, niveles[1]:j, niveles[2]:k} 
			for i, j, k in product([True, False],
						[True, False],
						[True, False])]

    niveles_dicts = [xi for xi in niveles_dicts
            if not (xi['cve_ent'] == False and
                xi['cve_muni'] == True)]

    #loop through all aggregations
    exito = []
    for agg_dict in niveles_dicts:
        print(agg_dict)
        exito.append(pub_agrupaciones(spark, df, output_path, year, agg_dict))

    # if(all(exito)):
    #    f = open("exitoso.txt","w+")
    #    f.write("exito")
    #    s3 = boto3.resource('s3')
    #    s3.Object(output_path, 'exitoso.txt').put(Body=open('exitoso.txt', 'rb'))
