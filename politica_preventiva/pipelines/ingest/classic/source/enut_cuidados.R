args = commandArgs(trailingOnly=TRUE)

library(readxl)
library(tidyverse)
library(foreign)
library(glue)

year <- "2014"

ifelse(substr(year,3,3) == "0", year_ab<- year, year_ab<-substr(year,3,4))

## Descarga de datos

download.file(glue("http://www.beta.inegi.org.mx/contenidos/proyectos/enchogares/especiales/enut/{year}/microdatos/bd_enut{year_ab}_dbf.zip"),destfile = (glue("bd_enut{year_ab}_dbf.zip")),mode="wb")
unzip("bd_enut14_dbf.zip", list = FALSE)
unzip(glue("bd_enut{year_ab}_dbf.zip"), (glue("Enut{year}_Tradicional.zip")), list = FALSE)
list (glue("Enut{year}_Tradicional.zip"))
unzip(glue("Enut{year}_Tradicional.zip"), list = FALSE)
read.dbf("TSDem.DBF")
read.dbf("TModulo2.DBF")
TModulo2 <- (read.dbf("TModulo2.DBF"))
TModulo2$N_REN <- as.character(TModulo2$N_REN)
TSDem <- (read.dbf("TSDem.DBF"))
TSDem$N_REN <- as.character(TSDem$N_REN)
tiempo_cuidado <- full_join(TModulo2, TSDem, by=c("CONTROL", "N_REN", "VIV_SEL", "HOGAR", "TLOC", "UPM_DIS", "EDIS", "FAC_PER"))


write_delim(x = tiempo_cuidado, path = args[3], delim='|', na = "")
