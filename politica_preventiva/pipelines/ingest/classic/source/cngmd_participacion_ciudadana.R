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
db <- data.frame(read.dbf("Bases_Datos/CONS_CIU.DBF"))
db <- mutate_all(db, function(x) gsub('NA','',x))

write_delim(x = db, path = args[3], delim='|', na = "")
