#!/usr/bin/env bash

########################
# CUAPS: criterion-level
########################

# Este script ingesta los datos del CUAPS a nivel criterio de focalización. 

# Se toman como parámetros, en ese orden, el data day, el directorio de ingesta y el output path.

## Pendiente: incluir fecha cuando se actualicen los nombres de archivos.

echo 'Downloading CUAPS:criterion-level data'

aws s3 cp s3://sedesol-lab/CUAPS-PROGRAMAS/BDCUAPS_FOCALIZACION.xlsx $2/cuaps_criterios.xlsx

in2csv --sheet 'BD_FOCALIZACION' $2/cuaps_criterios.xlsx | \
csvformat -D '|' > $3

rm $2/cuaps_criterios.xlsx
