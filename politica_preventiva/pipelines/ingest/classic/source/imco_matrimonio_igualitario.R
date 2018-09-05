args = commandArgs(trailingOnly = TRUE)

library(tidyverse)
library(aws.s3)
library(readxl)

#library(dotenv)
#dotenv::load_dot_env()

# Descarga de datos

datos = s3read_using(read_xlsx, object = "CAPACIDADES-INFORM/48) 2016_Sociedad_Matrimonio igualitario.xlsx",bucket = "sedesol-lab")
datos <- mutate_all(datos, function(x) gsub('\n','',x))
datos <- mutate_all(datos, function(x) gsub('\r','',x))
datos <- mutate_all(datos, function(x) gsub('\t','',x))
datos = datos[-c(33,34),]
colnames(datos)[c(2,3)] <- c("legislacion_reconoce_matrimonio_igualitario",
                          "fundamento_articulo")
# Escribir csv

write_delim(datos,path=args[3], delim = "|")