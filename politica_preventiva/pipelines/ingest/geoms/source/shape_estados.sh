#!/bin/bash

curl -o estados.zip http://mapserver.inegi.org.mx/MGN/mge2010v5_0.zip
unzip estados.zip
mkdir -p estados
mv Entidades* estados
#sudo apt-get install gdal-bin
cd estados
ogr2ogr states.shp Entidades_2010_5.shp -t_srs "+proj=longlat +ellps=WGS84 +no_defs +towgs84=0,0,0"
