#!/usr/bin/env bash

########################
# CUAPS: component-level
########################

# Este script ingesta los datos del CUAPS a nivel apoyo. Uno o más apoyos pueden integrar una componente de algún programa.
# Se toman como parámetros, en ese orden, el data day, el directorio de ingesta y el output path.

## Pendiente: incluir fecha cuando se actualicen los nombres de archivos

echo 'Downloading CUAPS:component-level data'

aws s3 cp s3://sedesol-lab/CUAPS-PROGRAMAS/BDCUAPS_APOCOMP_2.csv $3 

echo 'Downloaded to: '$3 
