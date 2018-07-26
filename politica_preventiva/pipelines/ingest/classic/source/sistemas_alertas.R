args <- commandArgs(trailingOnly = TRUE)

library(readxl)
library(tidyverse)
library(gsubfn)

#data_date <- args[1]
data_date <- "2016"

# Descarga de Datos
tmp = tempfile(fileext = ".xlsx")
url_base_1 <- 'http://disciplinafinanciera.hacienda.gob.mx/work/models/DISCIPLINA_FINANCIERA/Documentos/SistemaAlertas/'
url_base_2 <- '/CP/Informaci%C3%B3n%20Variables%20SdeA%20Entidades%20Federativas.xlsx'
get_liquidez <- function(i){
  print(i)
  anio <- as.character(data_date[i])
  url <- paste(url_base_1,url_base_2, sep = anio)
  
  out <- tryCatch(
    {
      download.file(url = url, destfile = tmp, mode="wb")
      data <- read_excel(tmp)
      data
    },
    error = function(cond){
      message(paste("File not found",url))
      return(empty)
    },
    finally={
      message(paste("Processed URL:",url))
      message("Data will be appended.")
    }
  )
  out
}

# Transformación y limpieza"
liquidez <- map_df(.x = length(data_date), .f=get_liquidez)
colnames(liquidez) <- liquidez[7,]
colnames(liquidez)[c(10:17,19:26,28,29)] <- liquidez[9,c(10:17,19:26,28,29)]
liquidez <- liquidez[-c(1:9,41:56),-c(14,23)]
colnames(liquidez)[c(4,6,8)] <- c("Indicador1","Indicador2","Indicador3")
colnames(liquidez) <- gsubfn(".", list("á" = "a", "é" = "e","í" = "i","ó" = "o", "ú" = "u"," " = "","\t"="","\n"="","\r"="", ":"="","."=""), colnames(liquidez))
liquidez$EntidadFederativa <- map_chr(.x = liquidez$EntidadFederativa, .f = function(x){gsub(" 1/" ,"", x=x)})
write_delim(liquidez,path=args[3], delim = "|")
