#!/usr/bin/env Rscript
args = commandArgs(trailingOnly=TRUE)

library(tidyverse)
library(rvest)
library(rvest)
library(stringr)

#print(args)
data_date <- args[1]

url_base <- 'http://www.economia-sniim.gob.mx/Nuevo/Consultas/MercadosNacionales/PreciosDeMercado/Agricolas/ResultadosConsultaFechaGranos.aspx?Semana=%s&Mes=%s&Anio=%s&ProductoId=605&OrigenId=-1&Origen=Todos&DestinoId=-1&Destino=Todos&RegistrosPorPagina=%%20500'

get_corn_prices <- function(i){
  print(i)
  anio <- fechas$anio[i]
  mes <- fechas$mes[i]
  semana <- fechas$semana[i]
  url <- sprintf(url_base,semana,mes,anio)
  empty <- data.frame(fecha=NULL,origen=NULL,destino=NULL,precio_min=NULL,precio_max=NULL,
                      precio_frec=NULL,obs=NULL,anio=NULL,mes=NULL,semana=NULL)
  out <- tryCatch(
    {
      data <- read_html(url) %>%
        html_nodes(xpath = '//*[(@id = "tblResultados")]') %>%
        html_table(fill = T, header = T)
      if(length(data) > 0){
        dat <- data[[1]]
        names(dat) <- c("fecha","origen","destino","precio_min","precio_max","precio_frec","obs")
        dat[dat$destino=="", "destino"] <- NA
        dat$obs <- as.character(dat$obs)
        dat$precio_min <- as.numeric(dat$precio_min)
        dat$precio_max <- as.numeric(dat$precio_max)
        dat$precio_frec <- as.numeric(dat$precio_frec)
        dat$origen <- as.character(dat$origen)
        dat$destino <- as.character(dat$destino)
        dat$origen <- str_replace_all(dat$origen,'"','')
        dat$destino <- str_replace_all(dat$destino,'"','')
        dat <- dat %>% fill(destino)
        dat$anio <- anio
        dat$mes <- mes
        dat$semana <- semana
      }else{
        dat <- empty
      }
      dat
    },
    error=function(cond) {
      message(paste("empty table:", url))
      return(empty)
    },
    finally={
      message(paste("Processed URL:", url))
      message("Data will be appended.")
    }
  )    
  out
}

# Descarga de Datos
semanas <- 1:4
fecha <- str_split(data_date,"-")[[1]]
anio <- fecha[1]
mes <- fecha[2]

fechas <- expand.grid(anio=anio, mes=mes, semana=semanas)
maiz_precios <- map_df(.x = 1:nrow(fechas), .f = get_corn_prices)
write_delim(x = maiz_precios, path = args[3], delim='|')
