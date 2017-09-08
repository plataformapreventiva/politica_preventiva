#!/bin/bash

###############
# Datos de Mortalidad
# Defunciones Fetales
###############

echo "Defunciones Fetales"

year=$1
local_path=$2
local_ingest_file=$3

# Downloads
curl http://www.beta.inegi.org.mx/contenidos/proyectos/registros/vitales/mortalidad/microdatos/fetales/${year}/fetales_base_de_datos_${year}_dbf.zip > $local_path/defunciones_fetales_${year}.zip

echo 'unzip'
unzip -o -d $local_path/defunciones_fetales_${year}  $local_path/defunciones_fetales_${year}.zip

cut_year=$(echo $year|cut -c3-4)

file=$local_path/defunciones_fetales_${year}/FETAL${cut_year}.dbf
if [ ! -f $file ]; then
    file=$local_path/defunciones_fetales_${year}/FETAL${cut_year}.DBF
fi

in2csv "$file" > $local_path/tmp_${cut_year}.csv
csvformat -e 'utf-8' -D \| $local_path/tmp_${cut_year}.csv > $local_ingest_file

rm $local_path/tmp_${cut_year}.csv
rm -R $local_path/defunciones_fetales_${year}
rm $local_path/defunciones_fetales_${year}.zip
