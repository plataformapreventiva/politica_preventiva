#!/bin/bash
# Script para descargar geometrías de municipios de Inegi

# Download geoms de inegi

wget -O "$1/municipios.zip" http://internet.contenidos.inegi.org.mx/contenidos/Productos/prod_serv/contenidos/espanol/bvinegi/productos/geografia/marcogeo/889463142683_s.zip
unzip -o $1/municipios.zip 

ogr2ogr -f CSV -t_srs EPSG:4326 $1/geoms_municipios.csv $1/conjunto_de_datos/areas_geoestadisticas_municipales.shp  -lco GEOMETRY=AS_WKT |\
	csvformat -D '|' > $3

