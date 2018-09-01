args = commandArgs(trailingOnly = TRUE)

library(tidyverse)
library(aws.s3)
library(readxl)

#library(dotenv)
#dotenv::load_dot_env()

# Descarga de datos

datos = s3read_using(read_xlsx, object = "CAPACIDADES-INFORM/49) 2016_Político_Agresiones a periodistas.xlsx",bucket = "sedesol-lab")

# Transformación y limpieza

# Agresiones a periodistas
names <- c("entidad_federativa","agresiones_a_periodistas_2008","agresiones_a_periodistas_2009",
           "agresiones_a_periodistas_2010","agresiones_a_periodistas_2011","agresiones_a_periodistas_2012",
           "agresiones_a_periodistas_2013","agresiones_a_periodistas_2014","agresiones_a_periodistas_2015")
colnames(datos) <- names

# Escribir csv

write_delim(datos,path=args[3], delim = "|")