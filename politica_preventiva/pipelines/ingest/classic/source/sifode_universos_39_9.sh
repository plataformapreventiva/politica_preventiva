#!/usr/bin/env bash


###################################
# SIFODE: Universos
###################################

# Este script descarga los datos de Universos Potenciales del SIFODE (Sisteme de Focalización de Desarrollo)
# El script asume que se tiene acceso a la carpeta sifode-raw, en el S3 del proyecto.
# Se toman como parámetros, en ese orden, el data date, el directorio de ingesta y el output path

echo "Downloading Universos data.
Source: SIFODE"

year=$1
local_path=$2
local_ingest_file=$3

aws s3 cp s3://sifode-raw/universos_39.9.zip $local_path/universos_39_9.zip

echo 'Decompressing zip file'

CSVKIT_ENCODING='UTF-8'

gunzip -cd $local_path/universos_39_9.zip | \
sed -e 's/\r$// ; s/^\xef\xbb\xbf//' | \
csvformat -d '^' -D '|' -v > $local_ingest_file

echo 'Written to: '$local_ingest_file 
