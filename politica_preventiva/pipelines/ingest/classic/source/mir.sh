#!/bin/bash

echo 'Downloading MIR'

DATA_DIR=$1

declare -a array

# MIR
# 2016
array[1]="Reporte_CalifMIR__2016.zip"
# 2014
# array[2]="monitoreo/informes/Base_de_datos_MIR_Diagnostico_2014.zip"
# 2012
#array[3]="monitoreo/informes/diagnostico2012.zip"
# 2010
#array[4]="monitoreo/informes/diagnostico2010.zip"
# 2008
#array[5]="monitoreo/informes/diagnostico2008.zip"

# Diagnóstico de indicadores
#array[2]="Reporte_CalifIND__2016.zip"
#array[4]="monitoreo/informes/Base_de_datos_Indicadores_Diagnostico_2014.zip"
#array[6]="monitoreo/informes/ind2012.zip"
#array[8]="monitoreo/informes/ind2010.zip"

for i in $(seq 1 ${#array[@]});
do
    echo '############## DOWNLOADING '${array[$i]}' ################'
    wget -U 'Mozilla/5.0 (X11; Linux x86_64; rv:30.0) Gecko/20100101 Firefox/30.0' \
    -O $DATA_DIR/tmp.zip  http://www.coneval.org.mx/coordinacion/Documents/${array[$i]}
    unzip $DATA_DIR/tmp.zip -d  $DATA_DIR/ && rm $DATA_DIR/tmp.zip;
    in2csv --no-inference  $DATA_DIR/Reporte_CalifMIR__2016.xlsx |\
    csvformat -D "|" > $2;
done
