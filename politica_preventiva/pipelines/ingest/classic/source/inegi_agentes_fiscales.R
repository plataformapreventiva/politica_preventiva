args = commandArgs(trailingOnly = TRUE)

library(aws.s3)
library(tidyverse)
library(readxl)
library(dplyr)

#dotenv::load_dot_env()

#Descarga de datos
data = s3read_using(read_xls, object = "CAPACIDADES-INFORM/CNI_CNI20180710112023.XLS",bucket = "sedesol-lab")

# Transformación y limpieza
colnames(data) <-data[12,]
data <- data[-c(1:12,112:118),]
data <- data %>% gather(anio,num_agentes_fiscales,`2010`:`2016`)
data$anio <- as.integer(data$anio)
data$num_agentes_fiscales <- as.numeric(data$num_agentes_fiscales)
data <- data %>% rename(entidad_federativa="Entidad federativa", indicador="Indicador")
write_delim(data, path=args[3], delim = "|", na = '')

