#!/usr/bin/env bash

####################################
# CUIS Histórico: Personas
####################################

# Este script descarga los datos de Personas del CUIS Histórico (Cuestionario Único de Información Socioeconómica)
# Se asume que se tiene acceso a la carpeta sedesol-lab, en el S3 del proyecto
# Se toman como parámetros, en ese orden, el data date, el directorio de ingesta y el output path

echo "Downloading Persona data. 
Source: CUIS Histórico"

year=$1
local_path=$2
local_ingest_file=$3

aws s3 cp s3://sedesol-lab/CUIS-HISTORICO/persona.zip $local_path/persona.zip

echo 'Decompressing zip file'

unzip -c $local_path/persona.zip | \
sed '1,2d' | 
csvformat -d '^' -D '|' -v > $local_ingest_file

echo 'Written to: '$local_ingest_file

rm $local_path/persona.zip
