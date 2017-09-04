#!/bin/bash

###############
# Finanzas Públicas
###############
#sudo apt-get install python-dev python-pip python-setuptools build-essential
#pip install csvkit

echo "Descarga Finanzas Públicas ESTATALES"

year=$1
local_path=$2
local_ingest_file=$3

# url para bajar finanzas públicas
curl http://www.beta.inegi.org.mx/contenidos/proyectos/registros/economicas/finanzas/datosabiertos/efipem_municipal_csv.zip > $local_path/finanzas_publicas_$year.zip

echo 'unzip'
unzip -o -d $local_path/finanzas_publicas_$year  $local_path/finanzas_publicas_$year.zip

file=$local_path/finanzas_publicas_$year/conjunto_de_datos/efipem_tr_cifra_$year.csv
# copy file to ingest file
if [ -f "$file" ];
then
    csvformat -e 'utf-8' -D \| "$file" > $local_ingest_file
else
    echo "File $file does not exist"
fi
# Remove zip and unzip file
rm $local_path/finanzas_publicas_$year.zip
rm -rf $local_path/finanzas_publicas_$year
