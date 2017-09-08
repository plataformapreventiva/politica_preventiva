#!/bin/bash

###############
# CONEVAL
# Indicadores de pobreza, 2010-2014 (nacional y estatal), bases de microdatos finales
###############

echo "Descarga CONEVAL ESTADOS"

year=$1
local_path=$2
local_ingest_file=$3

echo $year
if [ "$year" = 2010 ]
then
    name_file=Pobreza_2010_DA

elif [ "$year" = 2012 ]
then
    name_file=Pobreza_12_DA

elif [ "$year" = 2014 ]
then
    name_file=Pobreza_14_DA

else
    echo 'No information for that year'
    exit
fi 

curl http://www.coneval.org.mx/Informes/Pobreza/Datos_abiertos/Pobreza/${name_file}.zip > $local_path/coneval_$year.zip
unzip -d $local_path/  $local_path/coneval_$year.zip
csvformat -e 'latin1' -D \| ${local_path}/${name_file}.csv > $local_ingest_file

rm $local_path/coneval_$year.zip
rm $local_path/${name_file}.csv
