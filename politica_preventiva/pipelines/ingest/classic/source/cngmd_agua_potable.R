args = commandArgs(trailingOnly=TRUE)

library(readxl)
library(tidyverse)
library(glue)

year <- "2017"
year_1 <- as.character(as.numeric(year)-1)
  
url <-glue("http://www.beta.inegi.org.mx/contenidos/proyectos/censosgobierno/municipal/cngmd/{year}/microdatos/m5/Serv_agua_red_cngmd{year}_dbf.zip")
download.file(url, destfile = glue("Serv_agua_red_cngmd{year}_dbf.zip"), mode="wb")
con <- unz(description=glue("Serv_agua_red_cngmd{year}_dbf.zip"), filename=glue("Serv_agua_red_cngmd{year}_dbf/Base_Datos/secc_i_tr_m5_{year_1}.csv"))
db <- read.csv(con, sep=",")
write_delim(x = db, path = args[3], delim='|')