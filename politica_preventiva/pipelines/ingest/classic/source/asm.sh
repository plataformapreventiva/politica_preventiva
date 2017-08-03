#!/bin/bash

#DATA_DIR=../data

# Descarga datos y metadatos
wget -U 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_12_5) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/59.0.3071.115 Safari/537.36'\
    http://www.transparenciapresupuestaria.gob.mx/work/models/PTP/DatosAbiertos/Programas/sed/aspectos_susceptibles_mejora.csv \
    -O $1/aspectos_susceptibles_mejora.csv

#wget -U 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_12_5) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/59.0.3071.115 Safari/537.36' \
#	http://www.transparenciapresupuestaria.gob.mx/work/models/PTP/DatosAbiertos/Diccionarios/diccionario_asm.xlsx \
#    -O $DATA_DIR/diccionario_asm.xlsx


# Limpia metadatos
#in2csv -e iso-8859-1 -f xlsx $DATA_DIR/diccionario_asm.xlsx > $DATA_DIR/asm_metadata.csv
#awk -F, 'length>NF+1' $DATA_DIR/asm_metadata.csv > $DATA_DIR/asm_metadata2.csv
#csvcut --columns=1,2 $DATA_DIR/asm_metadata2.csv > $DATA_DIR/asm_metadata3.csv

#csvformat -D "|" $DATA_DIR/asm_metadata3.csv > $DATA_DIR/asm_metadata.csv
#rm $DATA_DIR/asm_metadata2.csv; rm $DATA_DIR/asm_metadata3.csv;
#rm $DATA_DIR/diccionario_asm.xlsx

# Los metadatos fueron actualizados en 2015
#sed -i -e 's/_2015//g' $DATA_DIR/asm_metadata.csv

# Formato a datos
csvformat -e iso-8859-1 -D "|" $1/aspectos_susceptibles_mejora.csv > $2
rm $1/aspectos_susceptibles_mejora.csv