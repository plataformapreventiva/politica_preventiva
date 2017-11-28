#!/usr/bin/env bash

###############
# Geom
# Red carretera INEGI
###############

echo "Red de Carreteras INEGI"

year=$1
local_path=$2
local_ingest_file=$3

# Downloads
if [ $year = '2016' ]
then
    url = 'http://internet.contenidos.inegi.org.mx/contenidos/Productos/prod_serv/contenidos/espanol/bvinegi/productos/geografia/caminos/2016/702825219000_s.zip'
elif [ $year = '2015' ]
then
    url = "http://internet.contenidos.inegi.org.mx/contenidos/Productos/prod_serv/contenidos/espanol/bvinegi/productos/geografia/caminos/2015/702825209575_s.zip"
elif [ $year = '2014' ]
then
    url = "http://internet.contenidos.inegi.org.mx/contenidos/Productos/prod_serv/contenidos/espanol/bvinegi/productos/geografia/caminos/702825278724.zip"
else
    echo 'url not defined for the selected year'
    exit 1
fi

curl url > $local_path/red_carreteras.zip

echo 'unzip'
unzip -o -d $local_path/red_carreteras $local_path/red_carreteras.zip

if [ $year = '2014' ]
then
    mv $local_path/red_carreteras/producto/informaciвn\ espacial/ $local_path/data
    cd $local_path/data
    rename -v s/Red_Vial/red_vial/ Red_Vial.*
    cd -
elif [ $year = '2016' ]
    mkdir $local_path/data
    unzip -o -d $local_path/data/ $local_path/conjunto_de_datos/red_nacional_de_caminos_2016.zip
else
    mv $local_path/red_nacional_de_caminos_2015/conjunto_de_datos $local_path/data
fi

ogr2ogr -f "CSV" -t_srs EPSG:4326 $local_ingest_file $local_path/data/red_vial.shp -lco GEOMETRY=AS_WKT
#rm -r $local_path/data
find $local_path -type f -not -name "red_vial*" -print0 | xargs -0 rm --
