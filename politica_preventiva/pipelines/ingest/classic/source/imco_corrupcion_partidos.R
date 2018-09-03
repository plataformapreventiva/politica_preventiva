args = commandArgs(trailingOnly = TRUE)

library(tidyverse)
library(aws.s3)
library(readxl)

#library(dotenv)
#dotenv::load_dot_env()

# Descarga de datos

data4 = s3read_using(read_xlsx, object = "CAPACIDADES-INFORM/44) 2016_Politico_Participación ciudadana.xlsx",bucket = "sedesol-lab")

# Corrupción partidos políticos

colnames(data4) <- data4[1,]
data4 <- data4[-1,-1]
data4_2006 <- data4[1:32,]
data4_2012 <- data4[37:68,]
data4 <- left_join(data4_2006,data4_2012,by="ENTIDAD FEDERATIVA")
names <-c("ENTIDAD FEDERATIVA","SECCIONES_2006","CASILLAS_2006","PAN_2006","PRI_2006","PRD_2006",
          "PVEM_2006","PT_2006","MOVIMIENTO CIUDADANO_2006","NUEVA ALIANZA_2006",
          "COALICIÓN PRI PVEM_2006","COALICIÓN PRD PT MC_2006","COALICIÓN PRD PT_2006",
          "COALICIÓN PRD MC_2006","COALICIÓN PT MC_2006","NO REGISTRADOS_2006","NULOS_2006",
          "TOTAL_2006","LISTA NOMINAL_2006","PARTICIPACION_2006","OBSERVACIONES_2006","SECCIONES_2012",
          "CASILLAS_2012", "PAN_2012","ALIANZA POR MÉXICO_2012","POR EL BIEN DE TODOS_2012",
          "NUEVA ALIANZA_2012","ALTERNATIVA_2012","NO REGISTRADOS_2012","VÁLIDOS_2012",
          "NULOS_2012","TOTAL_2012","LISTA NOMINAL_2012","PARTICIPACION_2012")
colnames(data4) <- names
datos <- data4[,-c(35:41)]
rm(data4,data4_2006,data4_2012)

# Escribir csv

write_delim(datos,path=args[3], delim = "|")