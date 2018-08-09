#!/usr/bin/env bash

##############################################
# Delitos del fuero común: nivel municipal
##############################################

# Este script ingesta los datos de delitos del fuero común a nivel municipal
# Fuente: Secretariado Ejecutivo del Sistema Nacional de Seguridad Pública

# Se toman como parámetros de entrada, en ese orden: el data day, el directorio de ingesta y el output path

period=$1
local_path=$2
local_ingest_file=$3

echo "Downloading victims data.
Source: SENSP
Data day: "$period

wget http://secretariadoejecutivo.gob.mx/docs/pdfs/nueva-metodologia/Estatal-V%C3%ADctimas-2015-2017.zip --output-document $local_path/Estatal-Victimas-2015-2017.zip

unzip $local_path/Estatal-Victimas-2015-2017.zip -d $local_path

in2csv --format xlsx $local_path"/Estatal-Vбctimas-2015-2017.xslx" | \
csvformat -D '|' > $local_ingest_file

echo "Written to: "$local_ingest_file

#rm $local_path/Estatal-*
