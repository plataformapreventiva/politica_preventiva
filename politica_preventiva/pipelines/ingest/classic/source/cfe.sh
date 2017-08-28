#!/bin/bash
###############
# CFE
###############

YEAR=$1
DATA_DIR=$2
LOCAL_INGEST_FILE=$3

echo "Download CFE Data"


if [ $YEAR = '2016' ]; then
    url='http://datos.cfe.gob.mx/Datos/Usuariosyconsumodeelectricidadpormunicipio.csv'
else 
    echo 'url not defined for the selected year'
    exit 1
fi


wget -U 'Mozilla/5.0 (X11; Linux x86_64; rv:30.0) Gecko/20100101 Firefox/30.0' \
	$url -O $DATA_DIR/cfe.csv
awk '{if (NR!=1) {print}}' $DATA_DIR/cfe.csv > ${DATA_DIR}/cfe2.csv
csvformat -e iso-8859-1 -D "|" $DATA_DIR/cfe2.csv > $LOCAL_INGEST_FILE
rm $DATA_DIR/cfe.csv; rm $DATA_DIR/cfe2.csv