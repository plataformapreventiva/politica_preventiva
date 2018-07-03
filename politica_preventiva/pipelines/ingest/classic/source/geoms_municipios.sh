#!/bin/bash
################################################
# Script para descargar geometrías de municipios de Inegi
################################################

year=$1
local_path=$2
local_ingest_file=$3

if [ $year = '2018' ]; then
    cve=889463526636_s.zip
elif [ $year = '2017' ]; then
    cve=889463171829_s.zip
else
    echo 'url not defined for the selected year'
    exit 1
fi 
# Download geoms de inegi
mkdir $local_path/$year
wget "http://internet.contenidos.inegi.org.mx/contenidos/Productos/prod_serv/contenidos/espanol/bvinegi/productos/geografia/marcogeo/"$cve"" -O $local_path/geom_inegi$year.zip
unzip -o $local_path/geom_inegi$year.zip -d $local_path/$year/

# Convertir a CSV
ogr2ogr -f CSV -t_srs EPSG:4326 $local_path/$year/temp.csv $local_path/$year/conjunto_de_datos/areas_geoestadisticas_municipales.shp  -lco GEOMETRY=AS_WKT

# Cambiar formato
csvformat -D '|' -z 99999999999 $local_path/$year/temp.csv > $local_ingest_file
# rm -Rf $local_path
