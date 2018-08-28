args = commandArgs(trailingOnly = TRUE)

library(tidyverse)
library(aws.s3)
library(readxl)
library(data.table)
library(stringi)

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

personas$NOM_MUN <- stri_trans_general(personas$NOM_MUN, "latin-ascii")
personas$NOM_ENT <- stri_trans_general(personas$NOM_ENT, "latin-ascii") 
personas$NOM_LOC <- stri_trans_general(personas$NOM_LOC, "latin-ascii") 
personas$NOM_MUN_ASI <- stri_trans_general(personas$NOM_MUN_ASI, "latin-ascii")  
personas$NOM_MUN_RES10 <- stri_trans_general(personas$NOM_MUN_RES10, "latin-ascii") 
personas$NOM_MUN_TRAB <- stri_trans_general(personas$NOM_MUN_TRAB, "latin-ascii") 


write_delim(personas,path=args[3], delim = "|", na= "")