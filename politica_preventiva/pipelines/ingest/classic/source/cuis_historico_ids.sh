#!/usr/bin/env bash

####################################
# CUIS Histórico: IDs
####################################

# Este script descarga los datos de IDs del CUIS Histórico (Cuestionario Único de Información Socioeconómica)
# Se asume que se tiene acceso a la carpeta sedesol-lab, en el S3 del proyecto
# Se toman como parámetros, en ese orden, el data date, el directorio de ingesta y el output path

echo "Downloading IDs data. 
Source: CUIS Histórico"

year=$1
local_path=$2
local_ingest_file=$3

echo 'llave_hogar_h|c_integrante|new_id' > $local_ingest_file

aws s3 cp s3://sedesol-lab/CUIS-HISTORICO/ids_66.zip $local_path/ids.zip

echo 'Decompressing zip file'

unzip -c $local_path/ids.zip | \
sed '1,2d' | 
csvformat -d '^' -D '|' -v >> $local_ingest_file

echo 'Written to: '$local_ingest_file

rm $local_path/ids.zip
