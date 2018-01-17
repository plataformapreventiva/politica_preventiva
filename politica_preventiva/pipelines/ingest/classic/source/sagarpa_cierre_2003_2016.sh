#!/bin/bash

echo "Descarga CIERRE DE PRODUCCION AGRICOLA CSVs (desde 2003 hasta 2016)"

# Descargar info de cierre de produccion agricola desde 2003 hasta 2016 en CSVs del SIAP

year=$1
echo $year
local_path=$2
local_ingest_file=$3

bandera=false
for i in `seq 2003 1 2016`;
do
    if [ "${i}" = ${year} ]
    then
        $bandera = true
    fi
done

# if [ $bandera = true ]
# then
# 	url=http://infosiap.siap.gob.mx/gobmx/datosAbiertos/ProduccionAgricola/Cierre_agricola_mun_${year}.csv
#     echo $url
# 	curl $url > $local_path/temp.csv
# 	csvformat -e 'latin1' -D '|' $local_path/temp.csv > $local_ingest_file
# else
# 	echo "El año está fuera de rango"
# fi

url=http://infosiap.siap.gob.mx/gobmx/datosAbiertos/ProduccionAgricola/Cierre_agricola_mun_${year}.csv
echo $url
curl $url > $local_path/temp.csv
csvformat -e 'latin1' -D '|' $local_path/temp.csv > $local_ingest_file
rm $local_path/temp.csv
