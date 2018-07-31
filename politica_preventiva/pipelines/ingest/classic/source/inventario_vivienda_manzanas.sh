#!/usr/bin/env bash
# Script para descargar inventario nacional de vivienda 
# Fuente: http://www.beta.inegi.org.mx/app/mapa/inv/


mkdir -p $2/estado_temp
# Download geoms por estado
for i in $(seq 1 32); do
	n=$( printf "%02d" $i)
	# descarga shapefile
    wget "http://www.beta.inegi.org.mx/contenidos/masiva/indicadores/inv/"$n"_Manzanas_INV2016_shp.zip" -O $2/estado_temp/estado.zip

	unzip -o $2/estado_temp/estado.zip -d $2/estado_temp/

	# Convertir geometría a CSV
	ogr2ogr -f CSV -t_srs EPSG:4326 $2/estado_temp/temp.csv $2/estado_temp/"$n"_Manzanas_INV2016.shp  -lco GEOMETRY=AS_WKT

	# Cambiar delim
	csvformat -D '|' -z 99999999999 $2/estado_temp/temp.csv > $2/"$n".csv
	rm -Rf $2/estado_temp/*
done

# Concatenar y guardar
nawk 'FNR==1 && NR!=1{next;}{print}' *.csv | awk -F'|' 'NF==76' >  $3
