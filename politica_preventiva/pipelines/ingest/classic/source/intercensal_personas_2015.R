args = commandArgs(trailingOnly = TRUE)

library(tidyverse)
library(aws.s3)
library(readxl)
library(data.table)


# Descarga de datos
personas_01 <- s3read_using(read_csv,
                            object = "VULNERABILIDADES-INFORM/Intercensal/Personas/TR_PERSONA01.CSV",
                            bucket = "sedesol-lab")
personas_02 <- s3read_using(read_csv, 
                            object = "VULNERABILIDADES-INFORM/Intercensal/Personas/TR_PERSONA02.CSV",
                            bucket = "sedesol-lab")
personas_03 <- s3read_using(read_csv,
                            object = "VULNERABILIDADES-INFORM/Intercensal/Personas/TR_PERSONA03.CSV",
                            bucket = "sedesol-lab")

personas <- list(personas_01, personas_02, personas_03) %>% 
  rbindlist(., fill = TRUE) %>% as_tibble()

write_delim(datos,path=args[3], delim = "|")