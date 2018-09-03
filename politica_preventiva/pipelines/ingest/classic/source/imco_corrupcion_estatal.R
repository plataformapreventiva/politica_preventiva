args = commandArgs(trailingOnly = TRUE)

library(tidyverse)
library(aws.s3)
library(readxl)

#library(dotenv)
#dotenv::load_dot_env()

# Descarga de datos
data = s3read_using(read_xlsx, object = "CAPACIDADES-INFORM/41) 2016_Político_Corrupción estatal.xlsx",bucket = "sedesol-lab")

# Transformación y limpieza

# Corrupción estatal

name <- "entidad_federativa"
colnames(data)[1] <- name
poblacion_11 <- data[10:42,1:2]
poblacion_13 <- data[60:92,1:2]
poblacion_15 <- data[112:144,1:2]
poblacion <- left_join(poblacion_11, poblacion_13, by = "entidad_federativa") %>% 
  left_join(poblacion_15, by = "entidad_federativa")
names <- c("entidad_federativa","poblacion_18_mas_2011","poblacion_18_mas_2013","poblacion_18_mas_2015")
colnames(poblacion) <- names
rm(poblacion_11,poblacion_13,poblacion_15)

data <- data[,-c(2,3,6,9,12,15,16)]
ent_corrupcion_2011 <- data[c(10:42),]
names <- c("entidad_federativa","muy_frecuente_absoluto_2011",
           "muy_frecuente_realtivo_2011","frecuente_absoluto_2011","frecuente_relativo_2011",
           "poco_frecuente_absoluto_2011","poco_frecuente_relativo_2011","nunca_absoluto_2011",
           "nunca_relativo_2011")
colnames(ent_corrupcion_2011) <- names
ent_corrupcion_2013 <- data[c(60:92),]
names <- c("entidad_federativa","muy_frecuente_absoluto_2013",
           "muy_frecuente_realtivo_2013","frecuente_absoluto_2013","frecuente_relativo_2013",
           "poco_frecuente_absoluto_2013","poco_frecuente_relativo_2013","nunca_absoluto_2013",
           "nunca_relativo_2013")
colnames(ent_corrupcion_2013) <- names
ent_corrupcion_2015 <- data[c(112:144),]
names <- c("entidad_federativa","muy_frecuente_absoluto_2015",
           "muy_frecuente_realtivo_2015","frecuente_absoluto_2015","frecuente_relativo_2015",
           "poco_frecuente_absoluto_2015","poco_frecuente_relativo_2015","nunca_absoluto_2015",
           "nunca_relativo_2015")
colnames(ent_corrupcion_2015) <- names
ent_corrupcion <- left_join(ent_corrupcion_2011,ent_corrupcion_2013, by = "entidad_federativa") %>% 
  left_join(ent_corrupcion_2015, by = "entidad_federativa")
rm(ent_corrupcion_2011,ent_corrupcion_2013,ent_corrupcion_2015)

datos <- left_join(poblacion,ent_corrupcion,by ="entidad_federativa")
rm(data,ent_corrupcion,poblacion)

# Escribir csv

write_delim(datos,path=args[3], delim = "|")