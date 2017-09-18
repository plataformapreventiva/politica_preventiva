
cript de exploración ETL PUB
"""
import argparse
import datetime
import sys

from __future__ import print_function
from pyspark import SparkContext
from pyspark.sql import functions as sf
from pyspark.sql.functions import unix_timestamp, dayofmonth, date_format, datediff, to_date, lit, col, udf, count, avg, sum

def trim(string):
    try:
        return string.strip()
    except:
        return ""

trim = udf(trim)

def aggregate_pub(year, input_path, output_path):

    """
    Creates csv file with pub . The information is divided by municipality.

    Args:
        (start_date): format 'year'
        (end_date) : format 'year'.

    Returns:
        Returns csv into s3 bucket.
    """

    #spark = SparkContext(appName="Pub Builder")

    ds = spark.read.csv( input_path + "pub_{0}.tar.gz".format(year), sep="|",
                        header=True, encoding="latin1")

    # Rename variables
    ds = (ds
          .withColumnRenamed('ANIO', 'anio_pago')
          .withColumnRenamed('NU_MES_PAGO', 'mes_pago')
          .withColumnRenamed('CVE_ENT', 'cve_ent')
          .withColumnRenamed('CVE_MUN', 'cve_mun')
          .withColumnRenamed('CVE_LOC', 'cve_locc')
          .withColumnRenamed('CVE_LOC', 'cve_locc')
          .withColumnRenamed('CD_PADRON', 'cve_padron')
          .withColumnRenamed('CD_PROGRAMA', 'cve_programa')
          .withColumnRenamed('CD_SEXO', 'sexo'))

    # Remove duplicates and na
    ds = ds.dropDuplicates()

    # Create Date Variables
    ds.registerTempTable('table')
    ds = spark.sql("""
      SELECT *, TO_DATE(CAST(UNIX_TIMESTAMP(FH_NACIMIENTO, 'yyyyMMdd')
       AS TIMESTAMP)) AS birthdate FROM table""")
    ds = ds.withColumn("age", (sf.datediff(
        to_date(lit(datetime.date.today())), sf.col("birthdate")))/360)

    ds = ds.withColumn("cve_muni", sf.concat(sf.col('cve_ent'), sf.lit(''),
        sf.col('CVE_MUN'))).withColumn("nombre",
        sf.concat(trim(sf.col('NB_NOMBRE')),
        sf.lit(' '), trim(
        sf.col('NB_PRIMER_AP')),
        sf.lit(' '), trim(sf.col('NB_SEGUNDO_AP'))))

    nombre = sf.when(sf.col("age") <= 18,
        "Menor de Edad").otherwise(sf.col("nombre"))

    categoria = sf.when(sf.col("age") >= 60, "Adulto Mayor").when((sf.col("age") <= 14) &
        (sf.col("age") >= 0), "Infante").when((sf.col("age") > 14) & (sf.col("age") < 60),
        "Adulto").otherwise(None)

    categoria_2 = sf.when(
        sf.col("age") < 0, "Error en captura de edad").otherwise(None)

    ds = ds.withColumn("categoria_edad", categoria)
    ds = ds.withColumn("error_edad", categoria)
    ds = ds.withColumn("nombre", nombre)

    # Nivel
    nivel = ds.groupby('anio_pago', 'mes_pago', 'cve_ent',
       'cve_muni', 'cve_padron').agg(
        avg("NU_IMP_MONETARIO").alias("promedio_importe"),
        count("*").alias("n_beneficiarios"),
        sum("NU_IMP_MONETARIO").alias("suma_importe"))

    nivel_ent = nivel.groupby('anio_pago', 'mes_pago', 'cve_ent',
       'cve_padron').agg(
        avg("promedio_importe").alias("promedio_importe"),
        sum("n_beneficiarios").alias("n_beneficiarios"),
        sum("suma_importe").alias("suma_importe"))

    nivel_sexo = ds.groupby('anio_pago', 'mes_pago', 'cve_ent',
        'cve_muni', 'sexo', 'cve_padron').agg(
        avg("NU_IMP_MONETARIO").alias("promedio_importe"),
        count("*").alias("n_beneficiarios"),
        sum("NU_IMP_MONETARIO").alias("suma_importe"))

    nivel_sexo_ent = nivel_sexo.groupby('anio_pago', 'mes_pago', 'cve_ent',
        'sexo', 'cve_padron').agg(
        avg("promedio_importe").alias("promedio_importe"),
        sum("n_beneficiarios").alias("n_beneficiarios"),
        sum("suma_importe").alias("suma_importe"))

    nivel_edad = ds.groupby('anio_pago', 'mes_pago', 'cve_ent',
        'cve_muni', 'categoria_edad', 'cve_padron').agg(
        avg("NU_IMP_MONETARIO").alias("promedio_importe"),
        count("*").alias("n_beneficiarios"),
        sum("NU_IMP_MONETARIO").alias("suma_importe"))

    nivel_edad_ent = nivel_edad.groupby('anio_pago', 'mes_pago', 'cve_ent',
        'categoria_edad', 'cve_padron').agg(
        avg("promedio_importe").alias("promedio_importe"),
        sum("n_beneficiarios").alias("n_beneficiarios"),
        sum("suma_importe").alias("suma_importe"))

    nivel_sexo = nivel_sexo.withColumnRenamed("sexo", "type")
    nivel_edad = nivel_edad.withColumnRenamed("categoria_edad", "type")
    nivel_sexo_ent = nivel_edad_ent.withColumnRenamed("sexo", "type")
    nivel_edad_ent = nivel_edad_ent.withColumnRenamed("categoria_edad", "type")

    nivel = nivel.withColumn("type", lit("total")).select("anio_pago",
        'mes_pago', "cve_ent","cve_muni", "type", "cve_padron",
        "promedio_importe", "n_beneficiarios", "suma_importe")
    nivel = nivel.unionAll(nivel_sexo.unionAll(nivel_edad)).withColumn("nivel", lit("municipal"))

    nivel_ent = nivel_ent.withColumn("type", lit("total")).select("anio_pago",
        'mes_pago', "cve_ent", "type", "cve_padron",
        "promedio_importe", "n_beneficiarios", "suma_importe")
    nivel_ent = nivel_ent.unionAll(nivel_sexo_ent.unionAll(nivel_edad_ent)).withColumn("nivel", lit("estado")).withColumn("cve_muni", lit("estatal"))

    nivel.unionAll(nivel_ent).write.csv(output_path + "/pub_{0}.csv".format(year),
     sep='|', header=True, mode='overwrite', encoding="utf-8")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='Download PUB and build level data')

    parser.add_argument('--start_date', type=int, default='2017',
        help= 'First year, as string format yyyy')

    parser.add_argument('--end_date', type=int, default=None,
        help= 'Last year as string format yyyy')

    parser.add_argument('--output_path', type=str, default='s3n://dpa-plataforma-preventiva/etl/pub/processing/',
        help = 'Name of outputfile')

    parser.add_argument('--input_path', type=str, default='s3n://pub-raw/',
        help = 'Name of outputfile')

    #parser.add_argument('--periodo', type=str, default='',
    #    help = 'Level of aggregation')

    args = parser.parse_args()

    start_date = args.start_date
    end_date = args.end_date
    output_path = args.output_path
    input_path = args.input_path
    #periodo = args.periodo

    if not end_date:
        end_year = int(start_year) + 1

    for year in range(start_date, end_date):

        ingest_pub_municipios(year=year, input_path=input_path,
            output_path=output_path)

