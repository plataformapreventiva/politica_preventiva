#!/usr/bin/env bash

########################
# Ingest script: nombre de la ingesta
########################

# Este script ingesta los datos de {fuente de datos} a nivel {unidad
# observacional, (i.e. nivel hogar/municipio, etc.)}

# {Aclarar acá los supuestos del código. Es decir: ¿se asume que se tiene acceso
# a alguna fuente de datos, como S3? ¿Se asume alguna estructura particular
# para el archivo (ejemplo: "Asumimos que el nombre siempre tiene esta
# forma...")}

# Notas sobre este script:
# 1. Todos los scripts de ingesta local se guardan en pipelines/ingest/classic/source
# 2. Los scripts de ingesta local están pensados como el primer paso para subir
# los datos crudos a nuestra base de datos, por lo que es importante hacer
# solamente el procesamiento mínimo necesario para esto. Es importante recordar
# que tenemos pipelines de limpieza y procesamiento de datos más adelante.

# Se toman como parámetros, en ese orden, el data day, el directorio de ingesta y el output path.

period=$1
local_path=$2
local_ingest_file=$3

echo 'Downloading data'

# Descargar el archivo desde su fuente remota
# (ejemplos de comandos para esta tarea: aws s3 cp, wget, curl...)

echo 'Ingesting CSV file for period: '$period

# Operaciones necesarias:
# Tomar el archivo descargado, pasarlo a formato CSV y guardarlo con pipes "|"
# como separador de columnas con el nombre de archivo indicado en
# $local_ingest_file
