#!/usr/bin/env bash

####################################
# Diccionario de programas sociales
####################################

# Este script ingesta el catalogo de programas carpeta pub-raw, en el S3 del proyecto

# Se toman como parámetros, en ese orden, el data day, el directorio de ingesta y el output path.

data_date=$1
local_path=$2
local_ingest_file=$3

echo 'Descarga diccionario'

aws s3 cp s3://pub-raw/diccionarios/pub-programas/catalogo_pub.csv $local_ingest_file

echo 'Written to: '$local_ingest_file
