#!/usr/bin/env Rscript
args = commandArgs(trailingOnly=TRUE)

library(readxl)
library(tidyverse)

print(args)
print(args[3])

# Descarga de Datos
tmp = tempfile(fileext = ".xlsx")
url <- 'http://smn.cna.gob.mx/tools/RESOURCES/Monitor%20de%20Sequia%20en%20Mexico/MunicipiosSequia.xlsx'
download.file(url = url, destfile = tmp, mode="wb")

# Transformación y limpieza
data <- read_excel(tmp)  
data %>% gather(12:length(colnames(data)),
                                     key = "fecha",
                                     value = "declaratoria",
                                     na.rm = TRUE) %>%
  mutate(fecha = as.Date(as.numeric(fecha), origin = "1899-12-30")) %>% 
  write_delim(args[3], delim='|')
