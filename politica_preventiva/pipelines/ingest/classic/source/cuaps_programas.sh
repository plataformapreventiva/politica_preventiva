#!/usr/bin/env bash

########################
# CUAPS: program-level
########################

# Este script ingesta los datos del CUAPS (Cuestionario Único de Aplicación a Programas) a nivel programa.

# Se asume que se tiene acceso a la carpeta sifode-raw, en el S3 del proyecto
# Se toman como parámetros, en ese orden, el data day, el directorio de ingesta y el output path.

period=$1
local_path=$2
local_ingest_file=$3

echo 'Downloading CUAPS:program-level data'

aws s3 cp s3://sedesol-lab/CUAPS-PROGRAMAS/BDCUAPS_PROGRAMA_3.xlsx $local_path/cuaps_programas.xlsx

echo 'Ingesting CSV file for period: '$period
in2csv --sheet 'BDCUAPS_PROGRAMA' $local_path/cuaps_programas.xlsx | \
csvformat -D '|' | \
sed -s '1 s/\./_/g' | \
sed -e 's/"^M"//g ; s/True/1/g ; s/False/0/g' > $local_ingest_file
echo 'Written to: '$local_ingest_file

rm $local_path/cuaps_programas.xlsx
