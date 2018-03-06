#!/usr/bin/env bash

####################################
# CUIS Histórico: Visita del encuestador
####################################

# Este script descarga los datos de visita del encuestador del CUIS Histórico (Cuestionario Único de Información Socioeconómica)
# Se asume que se tiene acceso a la carpeta sedesol-lab, en el S3 del proyecto
# Se toman como parámetros, en ese orden, el data date, el directorio de ingesta y el output path

echo "Downloading visita del encuestador data. 
Source: CUIS Histórico"

year=$1
local_path=$2
local_ingest_file=$3

aws s3 cp s3://sedesol-lab/CUIS-HISTORICO/visita_encuestador.zip $local_path/visita_encuestador.zip

echo 'Decompressing zip file'

unzip -c $local_path/visita_encuestador.zip | \
sed '1,2d' | \
sed -e '589092s/"^2"//' | \
csvformat --quoting 3 -d '^' -D '|' -v > $local_ingest_file

echo 'Written to: '$local_ingest_file

rm $local_path/visita_encuestador.zip
