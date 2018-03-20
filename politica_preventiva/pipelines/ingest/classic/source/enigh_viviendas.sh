#!/bin/bash

###############
# ENIGH
# 2010-2016 
###############

echo "Descarga CONEVAL ESTADOS"

year=$1
local_path=$2
local_ingest_file=$3

echo $year
if [ "$year" = 2012 ]
then
    name_file=http://www.beta.inegi.org.mx/contenidos/proyectos/enchogares/regulares/enigh/nc/2012/microdatos/ncv_viviendas_2012_concil_2010_csv.zip
    file=12
elif [ "$year" = 2014 ]
then
    name_file=http://www.beta.inegi.org.mx/contenidos/proyectos/enchogares/regulares/enigh/nc/2014/microdatos/NCV_Vivi_2014_concil_2010_csv.zip
    file=14
elif [ "$year" = 2016 ]
then
    name_file=http://www.beta.inegi.org.mx/contenidos/proyectos/enchogares/regulares/enigh/nc/2016/microdatos/enigh2016_ns_viviendas_csv.zip
    file=16
else
    echo 'No information for that year'
    exit
fi 

wget $name_file -O $local_path/viviendas_$year.zip
unzip $local_path/viviendas_$year.zip -d $local_path/viviendas_$year
csvformat -e 'latin1' -D '|'  $local_path/viviendas_$year/viviendas.csv >  $local_ingest_file 

rm $local_path/viviendas_$year.zip
rm -R $local_path/viviendas_$year/
