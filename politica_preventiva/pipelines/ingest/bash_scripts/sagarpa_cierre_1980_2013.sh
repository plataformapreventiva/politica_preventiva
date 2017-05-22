#!/bin/sh

echo "Descarga CIERRE DE PRODUCCION AGRICOLA CSVs (desde 1980 hasta 2013)"

# Descargar info de cierre de produccion agricola desde 1980 hasta 2013 en CSVs del SIAP
# A partir de entonces es necesario crawlear la info

mkdir -p ../data/sagarpa_cierre_1980_2013/


for i in $(cat ingest/bash_scripts/sagarpa_cierre_1980_2013.txt);
do 
wget ${url}${i} -P ../data/sagarpa_cierre_1980_2013/
done

