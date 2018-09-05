args = commandArgs(trailingOnly = TRUE)

library(tidyverse)
library(aws.s3)
library(readxl)

#library(dotenv)
#dotenv::load_dot_env()

# Descarga de datos

datos = s3read_using(read_xlsx, object = "CAPACIDADES-INFORM/46) 2016_Politico_Barreras candidatos independientes.xlsx",bucket = "sedesol-lab")

datos <- datos[-c(1,34:67),-1]
colnames(datos) <- c("Entidad","Gobernador","Ley o código","Artículos",
                     "Lista nominal o padrón","Fecha de consulta",
                     "Fecha de última versión","Misma ley del ICU",
                     "Fuente","Fuente Secundaria","dias_firmas_Gobernador",
                     "dias_firmas_Diputados","dias_firmas_Ayuntamiento",
                     "restricciones_temporales_Gobernador",
                     "restricciones_temporales_Diputados",
                     "restricciones_temporales_Ayuntamiento")
# Escribir csv

write_delim(datos,path=args[3], delim = "|")

