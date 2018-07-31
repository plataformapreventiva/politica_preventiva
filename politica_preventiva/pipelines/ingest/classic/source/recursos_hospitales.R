args = commandArgs(trailingOnly=TRUE)

library(readxl)
library(tidyverse)

local_path <- args[2]
zip_path <- paste0(local_path, "/CIERRES_ANUALES_SINERHIAS_01-15.zip")

## Descarga de datos

download.file("http://www.dgis.salud.gob.mx/descargas/zip/CIERRES_ANUALES_SINERHIAS_01-15.zip", destfile = zip_path, mode="wb")
unzip(zip_path, exdir = local_path, list = FALSE) 
recursos_hospitales <- read_excel(paste0(local_path, "/CIERRES_ANUALES_SINERHIAS_01-15/CIERRES_ANUALES_SINERHIAS_2001-2015.xlsx"), sheet = "2001-2015") %>%
                       mutate_if(is.character, function(x) gsub('\n', '', x))

write_delim(x = recursos_hospitales, path = args[3], delim='|')

