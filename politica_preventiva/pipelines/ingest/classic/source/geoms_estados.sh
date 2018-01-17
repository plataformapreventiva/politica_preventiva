#!/bin/bash
# Script para descargar geometrías de estados de Inegi-

# Download geoms de inegi
wget "http://internet.contenidos.inegi.org.mx/contenidos/Productos/prod_serv/contenidos/espanol/bvinegi/productos/geografia/marcogeo/889463142683_s.zip" -O $2/geoms_inegi.zip
unzip -o $2/geoms_inegi.zip -d $2/ 

# Convertir a CSV
ogr2ogr -f CSV -t_srs EPSG:4326 $2/temp.csv $2/conjunto_de_datos/areas_geoestadisticas_estatales.shp  -lco GEOMETRY=AS_WKT

# Limpiar formato
csvformat -D '|' -z 99999999999 $2/temp.csv > $3
