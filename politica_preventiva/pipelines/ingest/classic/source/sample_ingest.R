#!/usr/bin/env Rscript

######################################################
#Ingesta de {datos}
#Fuente: {fuente}
######################################################

#Este script ingesta los datos de {fuente de datos} a nivel {unidad
#observacional, (i.e. nivel hogar/municipio, etc.)}

#{Aclarar acá los supuestos del código. Es decir: ¿se asume que se tiene acceso
#a alguna fuente de datos, como S3? ¿Se asume alguna estructura particular
#para el archivo (ejemplo: "Asumimos que el nombre siempre tiene esta
#forma...")}

#Notas sobre este script:
#1. Todos los scripts de ingesta local se guardan en pipelines/ingest/classic/source
#2. Los scripts de ingesta local están pensados como el primer paso para subir
#los datos crudos a nuestra base de datos, por lo que es importante hacer
#solamente el procesamiento mínimo necesario para esto. Es importante recordar
#que tenemos pipelines de limpieza y procesamiento de datos más adelante.

args = commandArgs(trailingOnly=TRUE)

data_date <- args[1]
data_dir <- args[2]
local_ingest_file <- args[3]

# Insertar código que descargue los datos de su fuente remota y los cargue a
# memoria en un dataframe.

data <- data.frame()

# Escribir los datos a el archivo de ingesta local
write_delim(data, path = local_ingest_file, delim = '|', na = '')
