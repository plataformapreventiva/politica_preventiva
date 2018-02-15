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

echo "Downloading crime data.
Source: SENSP
Data day: "$period

wget http://secretariadoejecutivo.gob.mx/docs/pdfs/nueva-metodologia/Municipal-Delitos-2015-2017.zip --output-document $local_path/Municipal-Delitos-2015-2017.zip

unzip $local_path/Municipal-Delitos-2015-2017.zip -d $local_path
soffice --headless --convert-to csv $local_path/Municipal-Delitos-2015-2017.xlsb --outdir $local_path

iconv -f 'iso-8859-1' -t 'utf-8' $local_path/Municipal-Delitos-2015-2017.csv | \
csvformat -D '|' > $local_ingest_file

echo "Written to: "$local_ingest_file

#rm $local_path/Municipal-Delitos-2015-2017.*
