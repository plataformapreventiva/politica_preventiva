args = commandArgs(trailingOnly = TRUE)

library(tidyverse)
library(aws.s3)
library(readxl)

#library(dotenv)
#dotenv::load_dot_env()

# Descarga de datos

datos = s3read_using(read_xlsx, object = "CAPACIDADES-INFORM/47) 2016_Político_Equidad en el Congreso.xlsx",bucket = "sedesol-lab")

# Transformación y limpieza

# Equidad en el Congreso
datos <- datos[-c(1:3),-c(1,4,5,11,12)]
names <- c("entidad_federativa","participación_mujeres_julio_2014","número_diputados_julio_2015",
           "diputados_2015","diputadas_2015","porcen_diputados_2015","porcen_diputadas_2015",
           "número_diputados_julio_2016","diputados_2016","diputadas_2016","porcen_diputados_2016",
           "porcen_diputadas_2016")
colnames(datos) <- names

# Escribir csv

write_delim(datos,path=args[3], delim = "|")