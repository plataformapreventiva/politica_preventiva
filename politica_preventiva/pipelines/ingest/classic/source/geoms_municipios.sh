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

mkdir $local_path/$year $local_path/$year/temp $local_path/$year/final

## Download geoms de inegi
wget "http://internet.contenidos.inegi.org.mx/contenidos/Productos/prod_serv/contenidos/espanol/bvinegi/productos/geografia/marcogeo/"$cve"" -O $local_path/geom_inegi$year.zip

## Unzip folder principal
unzip -o $local_path/geom_inegi$year.zip -d $local_path/$year/

## Unzip folder por estado
for i in $local_path/$year/*.zip; do
  filename=$(basename -- "$i")
  filename="${filename%.*}"
  mkdir $filename
  unzip "$i" -d  $local_path/$year/$filename/
done

mv $local_path/$year/*/*/*mun* $local_path/$year/temp/
for i in $local_path/$year/temp/*.shp; do
  filename=$(basename -- "$i")
  filename="${filename%.*}"
  ogr2ogr -f CSV -t_srs EPSG:4326 $local_path/$year/final/$filename.csv $i  -lco GEOMETRY=AS_WKT
done

# Concatenar y guardar
nawk 'FNR==1 && NR!=1{next;}{print}'  $local_path/$year/final/*.csv > $local_path/$year/output.csv.file

# Cambiar formato
csvformat -D '|' -z 99999999999 $local_path/$year/output.csv.file > $local_ingest_file
rm -Rf $local_path/$year/
