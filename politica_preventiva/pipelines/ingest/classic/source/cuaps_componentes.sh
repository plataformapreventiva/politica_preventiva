#!/usr/bin/env bash

########################
# CUAPS: component-level
########################

# Este script ingesta los datos del CUAPS (Cuestionario Único de Aplicación a Programas)  a nivel apoyo. Uno o más apoyos pueden integrar una componente de algún programa.i
# El script asume que se tiene acceso a la carpeta sifode-raw, en el S3 del proyecto 

# Se toman como parámetros, en ese orden, el data day, el directorio de ingesta y el output path.

period=$1
local_path=$2
local_ingest_file=$3

echo 'Downloading CUAPS:component-level data'

aws s3 cp s3://sedesol-lab/CUAPS-PROGRAMAS/BDCUAPS_APOCOMP_3.xlsx $local_path/cuaps_componentes.xlsx

echo 'Ingesting CSV file for period: '$period

in2csv --sheet 'BDCUAPS_APOCOMP' $local_path/cuaps_componentes.xlsx | \
csvformat -D '|' | \
sed -e '1 s/\./_/g' | \
sed -e 's/"^M"//g ; s/True/1/g ; s/False/0/g' > $local_ingest_file

echo 'Written to: '$local_ingest_file
