args = commandArgs(trailingOnly = TRUE)

library(tidyverse)
library(aws.s3)
library(readxl)
library(data.table)
library(stringi)


# Descarga de datos
viviendas_01 <- s3read_using(read_csv,
                            object = "VULNERABILIDADES-INFORM/Intercensal/Viviendas/TR_VIVIENDA01.CSV",
                            bucket = "sedesol-lab")
viviendas_02 <- s3read_using(read_csv, 
                            object = "VULNERABILIDADES-INFORM/Intercensal/Viviendas/TR_VIVIENDA02.CSV",
                            bucket = "sedesol-lab")
viviendas_03 <- s3read_using(read_csv,
                            object = "VULNERABILIDADES-INFORM/Intercensal/Viviendas/TR_VIVIENDA03.CSV",
                            bucket = "sedesol-lab")

viviendas <- list(viviendas_01, viviendas_02, viviendas_03) %>% 
  rbindlist(., fill = TRUE) %>% as_tibble()

viviendas$NOM_MUN <- stri_trans_general(viviendas$NOM_MUN, "latin-ascii")
viviendas$NOM_ENT <- stri_trans_general(viviendas$NOM_ENT, "latin-ascii") 
viviendas$NOM_LOC <- stri_trans_general(viviendas$NOM_LOC, "latin-ascii") 

write_delim(viviendas,path=args[3], delim = "|", na= "")