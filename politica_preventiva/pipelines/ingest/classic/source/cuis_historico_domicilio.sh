#!/usr/bin/env bash

####################################
# CUIS Histórico: domicilios
####################################

# Este script descarga los datos de domicilios del CUIS Histórico (Cuestionario Único de Información Socioeconómica)
# Se asume que se tiene acceso a la carpeta sedesol-lab, en el S3 del proyecto
# Se toman como parámetros, en ese orden, el data date, el directorio de ingesta y el output path

echo "Downloading addresses data.
Source: CUIS Histórico"

year=$1
local_path=$2
local_ingest_file=$3

aws s3 cp s3://sedesol-lab/CUIS-HISTORICO/domicilio.zip $local_path/domicilio.zip

echo 'Decompressing zip file'

unzip -c $local_path/domicilio.zip | \
sed '1,2d' | \
sed -e '1s/^\xEF\xBB\xBF//' | \
sed -e 's/\(".*\)^\(.*"\)/\1\2/g' | \
iconv -c -t 'utf-8' | \
csvformat -d '^' -D '|' -v | \
awk -F'|' 'NF==91' > $local_ingest_file

echo 'Written to: '$local_ingest_file

rm $local_path/domicilio.zip
