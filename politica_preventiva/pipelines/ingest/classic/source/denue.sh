#!/bin/bash

# FIRST ARGUMENT: path to save temp files
# SECOND ARGUMENT: output file name

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

    wget -P $1 "http://www.beta.inegi.org.mx/contenidos/masiva/denue/denue_"${array[$i]}"_csv.zip"
    unzip -o $1"/denue_"${array[$i]}"_csv.zip" -d $1"/denue_"${array[$i]}

    # Borrar zip y otros csv files
    rm $1"/denue_"${array[$i]}"_csv.zip"
    rm $(find -name denue_diccionario_de_datos.csv)

    # Mover archivo de interes a carpeta inicial y borrar todo lo demas
    mv $(find $1/denue_${array[$i]}"/" -name "*.csv") $1"/"${array[$i]}".csv"
    rm -r $1"/denue_"${array[$i]}
done

# Crear array para juntar archivos
array=( "${array[@]/%/.csv}" )
array=( "${array[@]/#/$1/}" )

# Juntar archivos, añanidendo columna "base" que diga el nombre del archivo origen
csvstack -n base --filenames ${array[@]} > $2
