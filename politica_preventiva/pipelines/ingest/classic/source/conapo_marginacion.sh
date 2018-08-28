#!/usr/bin/env bash

########################
# CONAPO: Marginación
########################

# Este script ingesta los datos de la cartografía de marginación del CONAPO, a
# nivel municipio.

# Se toman como parámetros, en ese orden, el data day, el directorio de ingesta y el output path.

period=$1
local_path=$2
local_ingest_file=$3

echo 'Downloading CONAPO: cartografía de marginación'

download_url='http://www.conapo.gob.mx/work/models/CONAPO/Marginacion/Datos_Abiertos/Municipio/Base_Indice_de_marginacion_municipal_90-15.csv'

wget -qO- $download_url | \
iconv  -f 'ISO-8859-15' -t 'utf-8' | \
sed -e 's/-|Nacional|-/00|Nacional|00000/g' | \
csvformat -D '|' > $local_ingest_file
echo 'Written to: '$local_ingest_file
