args = commandArgs(trailingOnly = TRUE)

#library(dotenv)
library(tidyverse)
library(aws.s3)
library(readxl)

#dotenv::load_dot_env()

# Descarga de datos
data = s3read_using(read_xls, object = "CAPACIDADES-INFORM/CNI_CNI20180710112149.XLS",bucket = "sedesol-lab")

# Transformación y limpieza
colnames(data) <-data[12,]
data <- data[-c(1:12,46:49),]
datos <- data %>% gather(anio,num_jueces,`2010`:`2016`)
write_delim(datos,path=args[3], delim = "|")
