#!/usr/bin/env bash

###############
# Geom
# Red de carreteras INEGI
###############

echo "Red de Carreteras INEGI"

year=$1
local_path=$2
local_ingest_file=$3

# Downloads
if [ $YEAR = '2016' ]
then
    url = 'http://internet.contenidos.inegi.org.mx/contenidos/Productos/prod_serv/contenidos/espanol/bvinegi/productos/geografia/caminos/2016/702825219000_s.zip'
elif [ $YEAR = '2015' ]
then
    url = "http://internet.contenidos.inegi.org.mx/contenidos/Productos/prod_serv/contenidos/espanol/bvinegi/productos/geografia/caminos/2015/702825209575_s.zip"
elif [ $YEAR = '2014' ]
then
    url = "http://internet.contenidos.inegi.org.mx/contenidos/Productos/prod_serv/contenidos/espanol/bvinegi/productos/geografia/caminos/702825278724.zip"
else
    echo 'url not defined for the selected year'
    exit 1
fi

curl url > $local_path/red_carreteras.zip

echo 'unzip'
unzip -o -d $local_path/red_carreteras $local_path/red_carreteras.zip
