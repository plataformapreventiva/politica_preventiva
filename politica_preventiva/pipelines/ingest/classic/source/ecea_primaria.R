args = commandArgs(trailingOnly=TRUE)

library(readxl)
library(tidyverse)
library(foreign)

##Descarga de datos

download.file("http://www.inee.edu.mx/images/stories/2017/ecea/bases_de_datos/dbf/1.-ECEA_p_Estudiantes-2014.dbf", 
              destfile = "1.-ECEA_p_Estudiantes-2014.dbf", 
              mode = "wb")
ecea_primaria <- (read.dbf("1.-ECEA_p_Estudiantes-2014.dbf", 
                              as.is = TRUE))

write_delim(x = ecea_primaria, path = args[3], delim='|')



