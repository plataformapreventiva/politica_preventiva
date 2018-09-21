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
personas_04 <- s3read_using(read_csv,
                            object = "VULNERABILIDADES-INFORM/Intercensal/Personas/TR_PERSONA04.CSV",
                            bucket = "sedesol-lab")
personas_05 <- s3read_using(read_csv,
                            object = "VULNERABILIDADES-INFORM/Intercensal/Personas/TR_PERSONA05.CSV",
                            bucket = "sedesol-lab")
personas_06 <- s3read_using(read_csv,
                            object = "VULNERABILIDADES-INFORM/Intercensal/Personas/TR_PERSONA06.CSV",
                            bucket = "sedesol-lab")
personas_07 <- s3read_using(read_csv,
                            object = "VULNERABILIDADES-INFORM/Intercensal/Personas/TR_PERSONA07.CSV",
                            bucket = "sedesol-lab")
personas_08 <- s3read_using(read_csv,
                            object = "VULNERABILIDADES-INFORM/Intercensal/Personas/TR_PERSONA08.CSV",
                            bucket = "sedesol-lab")
personas_09 <- s3read_using(read_csv,
                            object = "VULNERABILIDADES-INFORM/Intercensal/Personas/TR_PERSONA09.CSV",
                            bucket = "sedesol-lab")
personas_10 <- s3read_using(read_csv,
                            object = "VULNERABILIDADES-INFORM/Intercensal/Personas/TR_PERSONA10.CSV",
                            bucket = "sedesol-lab")
personas_11 <- s3read_using(read_csv,
                            object = "VULNERABILIDADES-INFORM/Intercensal/Personas/TR_PERSONA11.CSV",
                            bucket = "sedesol-lab")
personas_12 <- s3read_using(read_csv,
                            object = "VULNERABILIDADES-INFORM/Intercensal/Personas/TR_PERSONA12.CSV",
                            bucket = "sedesol-lab")
personas_13 <- s3read_using(read_csv,
                            object = "VULNERABILIDADES-INFORM/Intercensal/Personas/TR_PERSONA13.CSV",
                            bucket = "sedesol-lab")
personas_14 <- s3read_using(read_csv,
                            object = "VULNERABILIDADES-INFORM/Intercensal/Personas/TR_PERSONA14.CSV",
                            bucket = "sedesol-lab")
personas_15 <- s3read_using(read_csv,
                            object = "VULNERABILIDADES-INFORM/Intercensal/Personas/TR_PERSONA15.CSV",
                            bucket = "sedesol-lab")
personas_16 <- s3read_using(read_csv,
                            object = "VULNERABILIDADES-INFORM/Intercensal/Personas/TR_PERSONA16.CSV",
                            bucket = "sedesol-lab")
personas_17 <- s3read_using(read_csv,
                            object = "VULNERABILIDADES-INFORM/Intercensal/Personas/TR_PERSONA17.CSV",
                            bucket = "sedesol-lab")
personas_18 <- s3read_using(read_csv,
                            object = "VULNERABILIDADES-INFORM/Intercensal/Personas/TR_PERSONA18.CSV",
                            bucket = "sedesol-lab")
personas_19 <- s3read_using(read_csv,
                            object = "VULNERABILIDADES-INFORM/Intercensal/Personas/TR_PERSONA19.CSV",
                            bucket = "sedesol-lab")
personas_20 <- s3read_using(read_csv,
                            object = "VULNERABILIDADES-INFORM/Intercensal/Personas/TR_PERSONA20.CSV",
                            bucket = "sedesol-lab")
personas_21 <- s3read_using(read_csv,
                            object = "VULNERABILIDADES-INFORM/Intercensal/Personas/TR_PERSONA21.CSV",
                            bucket = "sedesol-lab")
personas_22 <- s3read_using(read_csv,
                            object = "VULNERABILIDADES-INFORM/Intercensal/Personas/TR_PERSONA22.CSV",
                            bucket = "sedesol-lab")
personas_23 <- s3read_using(read_csv,
                            object = "VULNERABILIDADES-INFORM/Intercensal/Personas/TR_PERSONA23.CSV",
                            bucket = "sedesol-lab")
personas_24 <- s3read_using(read_csv,
                            object = "VULNERABILIDADES-INFORM/Intercensal/Personas/TR_PERSONA24.CSV",
                            bucket = "sedesol-lab")
personas_25 <- s3read_using(read_csv,
                            object = "VULNERABILIDADES-INFORM/Intercensal/Personas/TR_PERSONA25.CSV",
                            bucket = "sedesol-lab")
personas_26 <- s3read_using(read_csv,
                            object = "VULNERABILIDADES-INFORM/Intercensal/Personas/TR_PERSONA26.CSV",
                            bucket = "sedesol-lab")
personas_27 <- s3read_using(read_csv,
                            object = "VULNERABILIDADES-INFORM/Intercensal/Personas/TR_PERSONA27.CSV",
                            bucket = "sedesol-lab")
personas_28 <- s3read_using(read_csv,
                            object = "VULNERABILIDADES-INFORM/Intercensal/Personas/TR_PERSONA28.CSV",
                            bucket = "sedesol-lab")
personas_29 <- s3read_using(read_csv,
                            object = "VULNERABILIDADES-INFORM/Intercensal/Personas/TR_PERSONA29.CSV",
                            bucket = "sedesol-lab")
personas_30 <- s3read_using(read_csv,
                            object = "VULNERABILIDADES-INFORM/Intercensal/Personas/TR_PERSONA30.CSV",
                            bucket = "sedesol-lab")
personas_31 <- s3read_using(read_csv,
                            object = "VULNERABILIDADES-INFORM/Intercensal/Personas/TR_PERSONA31.CSV",
                            bucket = "sedesol-lab")
personas_32 <- s3read_using(read_csv,
                            object = "VULNERABILIDADES-INFORM/Intercensal/Personas/TR_PERSONA32.CSV",
                            bucket = "sedesol-lab")

personas <- list(personas_01, personas_02, personas_03, personas_04, personas_05, personas_06, personas_07, 
                 personas_08, personas_09, personas_10, personas_11, personas_12, personas_13, personas_14, 
                 personas_15, personas_16, personas_17, personas_18, personas_19, personas_20, personas_21, 
                 personas_22, personas_23, personas_24, personas_25, personas_26, personas_27, personas_28, 
                 personas_29, personas_30, personas_31, personas_32) %>% 
  rbindlist(., fill = TRUE) %>% as_tibble() 

personas$NOM_MUN <- stri_trans_general(personas$NOM_MUN, "latin-ascii")
personas$NOM_ENT <- stri_trans_general(personas$NOM_ENT, "latin-ascii") 
personas$NOM_LOC <- stri_trans_general(personas$NOM_LOC, "latin-ascii") 
personas$NOM_MUN_ASI <- stri_trans_general(personas$NOM_MUN_ASI, "latin-ascii")  
personas$NOM_MUN_RES10 <- stri_trans_general(personas$NOM_MUN_RES10, "latin-ascii") 
personas$NOM_MUN_TRAB <- stri_trans_general(personas$NOM_MUN_TRAB, "latin-ascii") 


write_delim(personas,path=args[3], delim = "|", na= "")