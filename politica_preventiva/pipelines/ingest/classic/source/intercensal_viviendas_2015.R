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
viviendas_04 <- s3read_using(read_csv,
                             object = "VULNERABILIDADES-INFORM/Intercensal/Viviendas/TR_VIVIENDA04.CSV",
                             bucket = "sedesol-lab")
viviendas_05 <- s3read_using(read_csv,
                             object = "VULNERABILIDADES-INFORM/Intercensal/Viviendas/TR_VIVIENDA05.CSV",
                             bucket = "sedesol-lab")
viviendas_06 <- s3read_using(read_csv,
                             object = "VULNERABILIDADES-INFORM/Intercensal/Viviendas/TR_VIVIENDA06.CSV",
                             bucket = "sedesol-lab")
viviendas_07 <- s3read_using(read_csv,
                             object = "VULNERABILIDADES-INFORM/Intercensal/Viviendas/TR_VIVIENDA07.CSV",
                             bucket = "sedesol-lab")
viviendas_08 <- s3read_using(read_csv,
                             object = "VULNERABILIDADES-INFORM/Intercensal/Viviendas/TR_VIVIENDA08.CSV",
                             bucket = "sedesol-lab")
viviendas_09 <- s3read_using(read_csv,
                             object = "VULNERABILIDADES-INFORM/Intercensal/Viviendas/TR_VIVIENDA09.CSV",
                             bucket = "sedesol-lab")
viviendas_10 <- s3read_using(read_csv,
                             object = "VULNERABILIDADES-INFORM/Intercensal/Viviendas/TR_VIVIENDA10.CSV",
                             bucket = "sedesol-lab")
viviendas_11 <- s3read_using(read_csv,
                             object = "VULNERABILIDADES-INFORM/Intercensal/Viviendas/TR_VIVIENDA11.CSV",
                             bucket = "sedesol-lab")
viviendas_12 <- s3read_using(read_csv,
                             object = "VULNERABILIDADES-INFORM/Intercensal/Viviendas/TR_VIVIENDA12.CSV",
                             bucket = "sedesol-lab")
viviendas_13 <- s3read_using(read_csv,
                             object = "VULNERABILIDADES-INFORM/Intercensal/Viviendas/TR_VIVIENDA13.CSV",
                             bucket = "sedesol-lab")
viviendas_14 <- s3read_using(read_csv,
                             object = "VULNERABILIDADES-INFORM/Intercensal/Viviendas/TR_VIVIENDA14.CSV",
                             bucket = "sedesol-lab")
viviendas_15 <- s3read_using(read_csv,
                             object = "VULNERABILIDADES-INFORM/Intercensal/Viviendas/TR_VIVIENDA15.CSV",
                             bucket = "sedesol-lab")
viviendas_16 <- s3read_using(read_csv,
                             object = "VULNERABILIDADES-INFORM/Intercensal/Viviendas/TR_VIVIENDA16.CSV",
                             bucket = "sedesol-lab")
viviendas_17 <- s3read_using(read_csv,
                             object = "VULNERABILIDADES-INFORM/Intercensal/Viviendas/TR_VIVIENDA17.CSV",
                             bucket = "sedesol-lab")
viviendas_18 <- s3read_using(read_csv,
                             object = "VULNERABILIDADES-INFORM/Intercensal/Viviendas/TR_VIVIENDA18.CSV",
                             bucket = "sedesol-lab")
viviendas_19 <- s3read_using(read_csv,
                             object = "VULNERABILIDADES-INFORM/Intercensal/Viviendas/TR_VIVIENDA19.CSV",
                             bucket = "sedesol-lab")
viviendas_20 <- s3read_using(read_csv,
                             object = "VULNERABILIDADES-INFORM/Intercensal/Viviendas/TR_VIVIENDA20.CSV",
                             bucket = "sedesol-lab")
viviendas_21 <- s3read_using(read_csv,
                             object = "VULNERABILIDADES-INFORM/Intercensal/Viviendas/TR_VIVIENDA21.CSV",
                             bucket = "sedesol-lab")
viviendas_22 <- s3read_using(read_csv,
                             object = "VULNERABILIDADES-INFORM/Intercensal/Viviendas/TR_VIVIENDA22.CSV",
                             bucket = "sedesol-lab")
viviendas_23 <- s3read_using(read_csv,
                             object = "VULNERABILIDADES-INFORM/Intercensal/Viviendas/TR_VIVIENDA23.CSV",
                             bucket = "sedesol-lab")
viviendas_24 <- s3read_using(read_csv,
                             object = "VULNERABILIDADES-INFORM/Intercensal/Viviendas/TR_VIVIENDA24.CSV",
                             bucket = "sedesol-lab")
viviendas_25 <- s3read_using(read_csv,
                             object = "VULNERABILIDADES-INFORM/Intercensal/Viviendas/TR_VIVIENDA25.CSV",
                             bucket = "sedesol-lab")
viviendas_26 <- s3read_using(read_csv,
                             object = "VULNERABILIDADES-INFORM/Intercensal/Viviendas/TR_VIVIENDA26.CSV",
                             bucket = "sedesol-lab")
viviendas_27 <- s3read_using(read_csv,
                             object = "VULNERABILIDADES-INFORM/Intercensal/Viviendas/TR_VIVIENDA27.CSV",
                             bucket = "sedesol-lab")
viviendas_28 <- s3read_using(read_csv,
                             object = "VULNERABILIDADES-INFORM/Intercensal/Viviendas/TR_VIVIENDA28.CSV",
                             bucket = "sedesol-lab")
viviendas_29 <- s3read_using(read_csv,
                             object = "VULNERABILIDADES-INFORM/Intercensal/Viviendas/TR_VIVIENDA29.CSV",
                             bucket = "sedesol-lab")
viviendas_30 <- s3read_using(read_csv,
                             object = "VULNERABILIDADES-INFORM/Intercensal/Viviendas/TR_VIVIENDA30.CSV",
                             bucket = "sedesol-lab")
viviendas_31 <- s3read_using(read_csv,
                             object = "VULNERABILIDADES-INFORM/Intercensal/Viviendas/TR_VIVIENDA31.CSV",
                             bucket = "sedesol-lab")
viviendas_32 <- s3read_using(read_csv,
                             object = "VULNERABILIDADES-INFORM/Intercensal/Viviendas/TR_VIVIENDA32.CSV",
                             bucket = "sedesol-lab")

viviendas <- list(viviendas_01, viviendas_02, viviendas_03, viviendas_04, viviendas_05, viviendas_06, viviendas_07, 
                 viviendas_08, viviendas_09, viviendas_10, viviendas_11, viviendas_12, viviendas_13, viviendas_14, 
                 viviendas_15, viviendas_16, viviendas_17, viviendas_18, viviendas_19, viviendas_20, viviendas_21, 
                 viviendas_22, viviendas_23, viviendas_24, viviendas_25, viviendas_26, viviendas_27, viviendas_28, 
                 viviendas_29, viviendas_30, viviendas_31, viviendas_32 %>% 
  rbindlist(., fill = TRUE) %>% as_tibble()

viviendas$NOM_MUN <- stri_trans_general(viviendas$NOM_MUN, "latin-ascii")
viviendas$NOM_ENT <- stri_trans_general(viviendas$NOM_ENT, "latin-ascii") 
viviendas$NOM_LOC <- stri_trans_general(viviendas$NOM_LOC, "latin-ascii") 

write_delim(viviendas,path=args[3], delim = "|", na= "")