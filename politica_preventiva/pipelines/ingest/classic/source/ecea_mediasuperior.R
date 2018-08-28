args = commandArgs(trailingOnly=TRUE)

library(readxl)
library(tidyverse)
library(foreign)

##Descarga de datos

ecea_mediasuperior <- download.file("http://www.inee.edu.mx/images/stories/2018/ECEA/Bases_de_datos_2016/EVOE_EMS_16_ECEA_ESTUDIANTES_ULTIMO_GRADO.sav", 
              destfile = "EVOE_EMS_16_ECEA_ESTUDIANTES_ULTIMO_GRADO.sav", 
              mode = "wb")


ecea_mediasuperior <- (read.spss("EVOE_EMS_16_ECEA_ESTUDIANTES_ULTIMO_GRADO.sav", 
                           to.data.frame = TRUE))  
i <- sapply(ecea_mediasuperior, is.factor)
ecea_mediasuperior[i] <- lapply(ecea_mediasuperior[i], as.character)


write_delim(x = ecea_mediasuperior, path = args[3], delim='|')
