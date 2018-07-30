#!/usr/bin/env bash

########################
# CUAPS: criterion-level
########################

# Este script ingesta los datos del CUAPS (Cuestionario Único de Aplicación a Programas)  a nivel acriterio de focalización. Los apoyos de cada programa pueden tener distintas combinaciones de criterios de focalización.
# El script asume que se tiene acceso a la carpeta sifode-raw, en el S3 del proyecto

# Se toman como parámetros, en ese orden, el data day, el directorio de ingesta y el output path.

period=$1
local_path=$2
local_ingest_file=$3

echo 'Downloading CUAPS:criterion-level data'

aws s3 cp s3://sedesol-lab/CUAPS-PROGRAMAS/BDCUAPS_FOCALIZACION_5.xlsx $local_path/cuaps_criterios.xlsx

echo 'Ingesting CSV file for period: '$period

in2csv --sheet 'BDCUAPS_FOCALIZACION' $local_path/cuaps_criterios.xlsx | \
csvformat -D '|' | \
sed -e '1 s/\./_/g' | \
awk -F '|' '{while (NF<19) {getline more; $0 = $0 " " more};print}' | \
cut -d '|' -f 1-19 | \
sed -e 's/"^M"//g ; s/True/1/g ; s/False/0/g' > $local_ingest_file

echo 'Written to: '$local_ingest_file
