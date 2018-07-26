#!/usr/bin/env bash

#################
# SIFODE: CUIS
#################

# Este script descarga los datos del CUIS (Cuestionario Único de Información Socioeconómica)
# Que forma parte de los datos del SIFODE (Sistema de Focalización de Desarrollo)

# El script asume que se tiene acceso a la carpeta sifode-raw, en el S3 del proyecto.
# Se toman como parámetros, en ese orden, el data day, el directorio de ingesta y el output path

echo "Downloading CUIS data.
Source: SIFODE"

year=$1
local_path=$2
local_ingest_file=$3

aws s3 cp s3://sifode-raw/CUIS_39_9.rar $local_path/CUIS_39_9.rar

echo 'Decompressing rar file'
unrar p -inul $local_path/CUIS_39_9.rar | \
iconv -c -t 'utf-8' | \
sed -e 's/\r$// ; 1 s/^\xef\xbb\xbf//' | \
csvformat -d '^' -D '|' -v | \
cut -d'|' -f-4,9- > $local_ingest_file
echo 'Written to: '$local_ingest_file
