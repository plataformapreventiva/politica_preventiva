args = commandArgs(trailingOnly=TRUE)

library(readxl)
library(tidyverse)
library(foreign)
library(glue)

#year <- args[1]
year <- "2017"

temporal <- tempfile()
download.file(glue::glue("http://www.beta.inegi.org.mx/contenidos/proyectos/censosgobierno/municipal/cngmd/{year}/microdatos/m2/Transparencia_cngmd{year}_dbf.zip"), temporal)
files = unzip(temporal, list=TRUE)$Name
unzip(temporal, files=files[grepl("dbf",files)])
db1 <- data.frame(read.dbf("Bases_Datos/PUBLINST.DBF"))
db2 <- data.frame(read.dbf("Bases_Datos/MEC_INST.DBF"))
db3 <- data.frame(read.dbf("Bases_Datos/PUBLESPE.DBF"))
db <- left_join(db1,db2, by = "UBIC_GEO")
dbas <- left_join(db,db3, by = "UBIC_GEO")
gsub("NA","",dbas)
write_delim(x = dbas, path = args[3], delim='|', na = "")
