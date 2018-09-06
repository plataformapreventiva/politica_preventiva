args = commandArgs(trailingOnly = TRUE)

library(tidyverse)
library(aws.s3)
library(readxl)

#library(dotenv)
#dotenv::load_dot_env()

# Descarga de datos
datos = s3read_using(read_xlsx, object = "CAPACIDADES-INFORM/45) 2016_Político_Competencia electoral.xlsx",bucket = "sedesol-lab")

# Transformación y limpieza
colnames(datos)[4] <- "Url"

# Escribir csv

write_delim(datos,path=args[3], delim = "|")