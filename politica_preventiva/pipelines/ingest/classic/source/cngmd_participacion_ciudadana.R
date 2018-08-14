args = commandArgs(trailingOnly=TRUE)

library(readxl)
library(tidyverse)
library(foreign)
library(glue)
library(plyr)

#year <- args[1]
year <- "2017"

temporal <- tempfile()
download.file(glue("http://www.beta.inegi.org.mx/contenidos/proyectos/censosgobierno/municipal/cngmd/{year}/microdatos/m2/Participacion_ciudada_cngmd{year}_dbf.zip"),temporal)
files = unzip(temporal, list=TRUE)$Name
unzip(temporal, files=files[grepl("dbf",files)])
db1 <- data.frame(read.dbf("Bases_Datos/CONS_CIU.DBF"))
db2 <- data.frame(read.dbf("Bases_Datos/CONS_MEC.DBF"))
db3 <- data.frame(read.dbf("Bases_Datos/CONS_ORG.DBF"))
db4 <- data.frame(read.dbf("Bases_Datos/PARTICIU.DBF"))
db5 <- data.frame(read.dbf("Bases_Datos/PROPUREC.DBF"))
db6 <- data.frame(read.dbf("Bases_Datos/TIPOAUTO.DBF"))
db <- join_all(list(db1,db2,db3,db4,db6,db6), by = "UBIC_GEO",type = "left")
rm(db1,db2,db3,db4,db5,db6)
write_delim(x = db, path = args[3], delim='|')
