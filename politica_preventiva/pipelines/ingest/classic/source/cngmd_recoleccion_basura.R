args = commandArgs(trailingOnly=TRUE)

library(readxl)
library(tidyverse)
library(glue)
library(gsubfn)

#year <- args[1]
year <- "2017"
year_1 <- as.character(as.numeric(year)-1)

url <-glue("http://www.beta.inegi.org.mx/contenidos/proyectos/censosgobierno/municipal/cngmd/{year}/microdatos/m6/Rec_RSU_cngmd{year}_dbf.zip")
download.file(url, destfile = glue("Rec_RSU_cngmd{year}_dbf.zip", mode="wb"))
con <- unz(description=glue("Rec_RSU_cngmd{year}_dbf.zip"), filename=glue("Rec_RSU_cngmd2017_dbf/Bases_Datos/secc_i_tr_m6_{year_1}.csv"))
db <- read_csv(con)
for (i in 1:84){
  db[,i] <- map_chr(.x = db[,i], .f = function(x){gsub("\n","", x=x)})
}
write_delim(x = db, path = args[3], delim='|', na="")