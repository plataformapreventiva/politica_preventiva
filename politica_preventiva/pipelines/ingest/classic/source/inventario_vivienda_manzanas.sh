#!/usr/bin/env bash
# Script para descargar inventario nacional de vivienda

# Download geoms por estado

mkdir -p $2/estado_temp

for i in $(seq 1 32); do
	n=$( printf "%02d" $i)
	wget "http://www.beta.inegi.org.mx/contenidos/masiva/indicadores/inv/"$n"_Manzanas_INV2016_shp.zip" -O $2/estado_temp/estado.zip

	unzip -o $2/estado_temp/estado.zip -d $2/estado_temp/

	# Convertir a CSV
	ogr2ogr -f CSV -t_srs EPSG:4326 $2/estado_temp/temp.csv $2/estado_temp/"$n"_Manzanas_INV2016.shp  -lco GEOMETRY=AS_WKT

	# Cambiar formato
	csvformat -D '|' -z 99999999999 $2/estado_temp/temp.csv > $2/"$n".csv
	rm -Rf $2/estado_temp/*
done

# Concatenar
cat $2/*csv > $3
