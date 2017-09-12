#!/bin/bash
###############
# CFE
###############

YEAR=$1
DATA_DIR=$2
LOCAL_INGEST_FILE=$3

echo "Download CFE Data"

if [ $YEAR = '2017' ]; then
    url='http://datos.cfe.gob.mx/Datos/Usuariosyconsumodeelectricidadpormunicipio.csv'
else 
    echo 'url not defined for the selected year'
    exit 1
fi

wget -U 'Mozilla/5.0 (X11; Linux x86_64; rv:30.0) Gecko/20100101 Firefox/30.0' \
	$url -O $DATA_DIR/cfe.csv

awk '{if (NR!=1) {print}}' $DATA_DIR/cfe.csv | sed -E 's/ {1,}/ /g;s/ ,/,/g;s/\|//g' > $DATA_DIR/cfe_usuarios.csv
awk 'NR==1{p=1} p; /Ventas/{exit}' $DATA_DIR/cfe_usuarios.csv | head -n -1 > $DATA_DIR/usuarios.csv
sed -e '1,/Ventas/d' $DATA_DIR/cfe.csv | sed -E 's/ {1,}/ /g;s/ ,/,/g;s/\|//g' >  $DATA_DIR/cfe_ventas.csv
csvcut -d "," -C 19 $DATA_DIR/cfe_ventas.csv > $DATA_DIR/ventas.csv
csvstack -d "," -g Usuarios,Ventas -n tipo $DATA_DIR/usuarios.csv $DATA_DIR/ventas.csv > $DATA_DIR/cfe2.csv
csvformat -e iso-8859-1 -D "|" $DATA_DIR/cfe2.csv > $DATA_DIR/cfe3.csv
sed -E 's/,//g' $DATA_DIR/cfe3.csv > $DATA_DIR/cfe4.csv
sed -E 's/ \|/\|/g' $DATA_DIR/cfe4.csv > $LOCAL_INGEST_FILE
rm $DATA_DIR/cfe_*; rm $DATA_DIR/usuarios.csv; rm $DATA_DIR/ventas.csv; rm $DATA_DIR/cfe*;
