#!/bin/bash

###############
# ITER
# PRincipales resultados del Censo por localidad
###############

echo "Descarga ITER 2010"

local_path=$1
local_ingest_file=$2

#curl http://www.beta.inegi.org.mx/contenidos/proyectos/enchogares/especiales/intercensal/${year}/microdatos/eic${year}_01_csv.zip > $local_path/finanzas_publicas_$year.zip
# 2010
curl http://www.beta.inegi.org.mx/contenidos/proyectos/ccpv/2010/microdatos/iter/00_nacional_2010_iter_zip.zip > $local_path/iter_2010.zip

echo 'unzip'
unzip -o -d $local_path/iter_2010  $local_path/iter_2010.zip

file=$local_path/iter_2010/ITER_nalDBF10/ITER_NALDBF10.dbf

in2csv "$file" > $local_path/tmp.csv
csvformat -e 'utf-8' -D \| $local_path/tmp.csv > $local_ingest_file

rm $local_path/tmp.csv
rm -R $local_path/iter_2010
rm $local_path/iter_2010.zip

# 2000
#curl http://www.beta.inegi.org.mx/contenidos/proyectos/ccpv/2000/datosabiertos/cgpv2000_iter_00_csv.zip
