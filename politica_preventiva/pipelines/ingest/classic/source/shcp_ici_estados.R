args <- commandArgs(trailingOnly = TRUE)

library(readxl)
library(tidyverse)

tmp = tempfile(fileext = ".xlsx")
url <- "http://www.transparenciapresupuestaria.gob.mx/work/models/PTP/DatosAbiertos/Entidades_Federativas/ici_historico.xlsx"
download.file(url = url, destfile = tmp, mode="wb")

# Transformación y limpieza
data <- read_excel(tmp)  
write_delim(x = data, path = args[3], delim='|')
