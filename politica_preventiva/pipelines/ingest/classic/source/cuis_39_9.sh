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

unrar p -inul $local_path/CUIS_39_9.rar | \
csvformat -d '^' -D '|' -e utf-8-sig | \ 
cut -d'|' -f-4,9- > local_ingest_file

