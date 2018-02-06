#!/usr/bin/env bash

########################
# CUAPS: program-level
########################

# Este script ingesta los datos del CUAPS a nivel programa. 

# Se toman como parámetros, en ese orden, el data day, el directorio de ingesta y el output path.

## Pendiente: incluir fecha cuando se actualicen los nombres de archivos.

echo 'Downloading CUAPS:program-level data'

aws s3 cp s3://sedesol-lab/CUAPS-PROGRAMAS/BDCUAPS_PROGRAMA_2.csv $3


