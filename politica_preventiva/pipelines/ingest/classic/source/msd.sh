#!/bin/bash

# Descarga datos y metadatos
#wget -U 'Mozilla/5.0 (X11; Linux x86_64; rv:30.0) Gecko/20100101 Firefox/30.0' \
#    http://www.transparenciapresupuestaria.gob.mx/work/models/PTP/DatosAbiertos/Diccionarios/msd_sed_diccionario.xlsx \
#    -O $1/msd_sed_diccionario.xlsx
wget -U 'Mozilla/5.0 (X11; Linux x86_64; rv:30.0) Gecko/20100101 Firefox/30.0' \
    http://www.transparenciapresupuestaria.gob.mx/work/models/PTP/DatosAbiertos/Programas/prog_valoracion_del_desempeno.csv \
    -O $1/prog_valoracion_del_desempeno.csv

# Limpia metadatos
#in2csv -e iso-8859-1 -f xlsx $DATA_DIR/msd_sed_diccionario.xlsx > $DATA_DIR/msd_metadata.csv
#awk -F, 'length>NF+1' $DATA_DIR/msd_metadata.csv > $DATA_DIR/msd_metadata2.csv
#csvcut --columns=1,2 $DATA_DIR/msd_metadata2.csv > $DATA_DIR/msd_metadata3.csv

#csvformat -D "|" $DATA_DIR/msd_metadata3.csv > $DATA_DIR/msd_metadata.csv
#rm $DATA_DIR/msd_metadata2.csv; rm $DATA_DIR/msd_metadata3.csv;
#rm $DATA_DIR/msd_sed_diccionario.xlsx

# Los metadatos fueron actualizados en 2015
#sed -i -e 's/_2015//g' $DATA_DIR/msd_metadata.csv

# Formato a datos
csvformat -e iso-8859-1 -D "|" $1/prog_valoracion_del_desempeno.csv > $2
rm $1/prog_valoracion_del_desempeno.csv

