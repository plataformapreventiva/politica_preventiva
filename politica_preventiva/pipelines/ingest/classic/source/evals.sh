#!/bin/bash

# Descarga datos y metadatos
wget -U 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_12_5) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/59.0.3071.115 Safari/537.36'\
    http://www.transparenciapresupuestaria.gob.mx/work/models/PTP/DatosAbiertos/Programas/sed/evaluaciones.csv \
    -O $1/evaluaciones.csv

#wget -U 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_12_5) AppleWebKit/539.36 (KHTML, like Gecko) Chrome/59.0.3071.115 Safari/537.36'\
#    http://www.transparenciapresupuestaria.gob.mx/work/models/PTP/DatosAbiertos/Diccionarios/diccionario_evaluaciones.xlsx \
#    -O $DATA_DIR/diccionario_evaluaciones.xlsx


# Limpia metadatos
#in2csv -e iso-8859-1 -f xlsx $DATA_DIR/diccionario_evaluaciones.xlsx > $DATA_DIR/eval_metadata.csv
#awk -F, 'length>NF+1' $DATA_DIR/eval_metadata.csv > $DATA_DIR/eval_metadata2.csv
#csvcut --columns=1,2 $DATA_DIR/eval_metadata2.csv > $DATA_DIR/eval_metadata3.csv

#csvformat -D "|" $DATA_DIR/eval_metadata3.csv > $DATA_DIR/eval_metadata.csv
#rm $DATA_DIR/eval_metadata2.csv; rm $DATA_DIR/eval_metadata3.csv;
#rm $DATA_DIR/diccionario_evaluaciones.xlsx

# Los metadatos fueron actualizados en 2017
#sed -i -e 's/_2017//g' $DATA_DIR/eval_metadata.csv

# Formato a datos
csvformat -e iso-8859-1 -D "|" $1/evaluaciones.csv > $2
rm $1/evaluaciones.csv
