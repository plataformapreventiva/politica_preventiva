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
    url="http://internet.contenidos.inegi.org.mx/contenidos/Productos/prod_serv/contenidos/espanol/bvinegi/productos/geografia/caminos/2016/702825219000_s.zip"
elif [ $year = '2015' ]
then
    url="http://internet.contenidos.inegi.org.mx/contenidos/Productos/prod_serv/contenidos/espanol/bvinegi/productos/geografia/caminos/2015/702825209575_s.zip"
elif [ $year = '2017' ]
then
    url="http://internet.contenidos.inegi.org.mx/contenidos/Productos/prod_serv/contenidos/espanol/bvinegi/productos/geografia/caminos/2017/889463171836_s.zip"

elif [ $year = '2014' ]
then
    url="http://internet.contenidos.inegi.org.mx/contenidos/Productos/prod_serv/contenidos/espanol/bvinegi/productos/geografia/caminos/702825278724_s.zip"
else
    echo 'url not defined for the selected year'
    exit 1
fi

curl $url > $local_path/red_carretera.zip

echo 'unzip'
unzip -o -d $local_path/temporal $local_path/red_carretera.zip

if [ $year = '2014' ]
then
    mv $local_path/temporal/producto/informa* $local_path/temp
    cd $local_path/temp
    rename -v s/Red_Vial/red_vial/ Red_Vial.*
    cd -
    ogr2ogr -t_srs EPSG:4326 -f CSV /vsistdout/ $local_path/temp/red_vial.shp -lco GEOMETRY=AS_WKT | csvformat -D "|" | cut --complement -d'|' -f8 > $local_ingest_file
elif [ $year = '2016' ]
then
    mkdir $local_path/temp
    unzip -o -d $local_path/temp/ $local_path/temporal/conjunto_de_datos/red_nacional_de_caminos_2016.zip
    ogr2ogr -t_srs EPSG:4326 -f CSV /vsistdout/ $local_path/temp/red_vial.shp -lco GEOMETRY=AS_WKT | csvformat -D "|" | awk 'BEGIN{FS="|"}{if (NF<8) {printf "%s",$0} else print}' | sed 's/\"N\/D|\"/N\/D/g' | sed 's///g' > $local_ingest_file
elif [ $year = '2015' ]
then
    mv $local_path/temporal/red_nacional_de_caminos_2015/conjunto_de_datos $local_path/temp
    ogr2ogr -t_srs EPSG:4326 -f CSV /vsistdout/ $local_path/temp/red_vial.shp -lco GEOMETRY=AS_WKT | csvformat -D "|" | sed -i -e 's/^d -1 M\n//g' > $local_ingest_file
elif [ $year = '2017' ]
then
    mv $local_path/temporal/conjunto_de_datos $local_path/temp
    ogr2ogr -t_srs EPSG:4326 -f CSV /vsistdout/ $local_path/temp/red_vial.shp -lco GEOMETRY=AS_WKT | csvformat -D "|" > $local_ingest_file
fi

#ogr2ogr -t_srs EPSG:4326 -f CSV /vsistdout/ $local_path/temp/red_vial.shp -lco GEOMETRY=AS_WKT | csvformat -D "|" > $local_ingest_file
#ogr2ogr -f "CSV" -t_srs EPSG:4326 $local_ingest_file $local_path/data/red_vial.shp -lco GEOMETRY=AS_WKT
rm -r $local_path/temp $local_path/temporal $local_path/red_carretera.zip
#find $local_path -type f -not -name "*red_carretera.csv" -print0 | xargs -0 rm --
