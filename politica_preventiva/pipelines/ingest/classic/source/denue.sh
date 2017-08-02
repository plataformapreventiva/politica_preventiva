#!/bin/bash

# FIRST ARGUMENT: output file name

# Create temp file
WORK_DIR=`mktemp -d`

echo 'Downloading DENUE'

declare -a array
array[1]="00_62"
array[2]="00_62_0116"
array[3]="62_25022015"
array[4]="00_62_1016"
array[5]="00_61"
array[6]="00_61_0116"
array[7]="00_61_1016"
array[8]="61_25022015"

for i in $(seq 1 8);
do
    echo '############## DOWNLOADING '${array[$i]}' ################'

    wget -P $WORK_DIR "http://www.beta.inegi.org.mx/contenidos/masiva/denue/denue_"${array[$i]}"_csv.zip"
    unzip -o $WORK_DIR"/denue_"${array[$i]}"_csv.zip" -d $WORK_DIR"/denue_"${array[$i]}

    # Borrar zip y otros csv files
    rm $WORK_DIR"/denue_"${array[$i]}"_csv.zip"
    find $WORK_DIR/ -regex '.*denue_diccionario.*' -delete

    # Mover archivo de interes a carpeta inicial y borrar todo lo demas
    find  $WORK_DIR/denue_${array[$i]}"/" -name "*.csv" -print0 | 
        while IFS= read -r -d $'\0' file; do 
            filename=${array[$i]}_$(basename ${file%.*}).csv; 
            mv $file  $WORK_DIR/$filename ; done
    rm -r $WORK_DIR"/denue_"${array[$i]}

done

# Crear array para juntar archivos
array=$( find  $WORK_DIR/ -regex '.*csv' ) 

# Juntar archivos, añanidendo columna "base" que diga el nombre del archivo origen
csvstack -n base -e utf-8 --filenames $array | csvformat -d "," -D "|" | sed 's/"//g' > $2
