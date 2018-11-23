args = commandArgs(trailingOnly = TRUE)

library(tidyverse)
library(aws.s3)
library(readxl)

# Descarga de datos
datos = s3read_using(read_xlsx, object = "CAPACIDADES-INFORM/ICE 2016 Base de datos.xlsx",bucket = "sedesol-lab",sheet='Puntaje y posición (por año)')

# Transformación y limpieza
pesos <- datos[2,4:14]
ice <- datos[4:35,1:13]
ice_2 <- datos[4:35,15:27]
colnames(ice) <-datos[3,1:13]
colnames(ice_2) <-datos[3,1:13]
ice <- rbind(ice,ice_2) 
anio <- c(rep(2014,32),rep(2016,32))
ice <- cbind(ice,anio)

write_delim(ice,path=args[3], delim = "|")
