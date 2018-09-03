args = commandArgs(trailingOnly = TRUE)

library(tidyverse)
library(aws.s3)
library(readxl)

#library(dotenv)
#dotenv::load_dot_env()

# Descarga de datos

datos = s3read_using(read_xlsx, object = "CAPACIDADES-INFORM/43) 2016_Politico_Disponibilidad de información pública.xlsx",bucket = "sedesol-lab")

# Transformación y limpieza
# Disponibilidad de información pública

datos <- datos[-c(33:35),]

# Escribir csv

write_delim(datos,path=args[3], delim = "|")
