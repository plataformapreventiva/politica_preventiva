#!/bin/bash
#########################
### Declaratoria de Emergencia
#########################
# Fuente: https://datos.gob.mx/busca/dataset/declaratorias-sobre-emergencia-desastre-y-contingencia-climatologica/resource/41444ebe-6a35-4631-8f91-9237d5114488

year=$1
local_path=$2
local_ingest_file=$3

# Descarga de datos - declaratoria de emergencia
if [ "$year" = 2016 ]
then
    wget -q -O-  'https://drive.google.com/uc?export=download&id=0B5wc002kIlphVnBLUmk0NFl1U2c' |\
        iconv -f ISO-8859-1 -t UTF-8 | csvformat -D '|' | cut -d"|" -f-10 > $local_ingest_file
fi
