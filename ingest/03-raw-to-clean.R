###############################################################################
# RAW to Clean 
###############################################################################

rm(list=ls())
suppressPackageStartupMessages({
  library(jsonlite)
  library(ggplot2)
  library(tidyverse)
  library(lubridate)
  require(zoo)
  library(stringr)
  library('RPostgreSQL')
  source("../shameful/utils_shameful.R")
  source("./utilsAPI.R")
  library(clusterSim)
  library(foreign)
})


## Clean ##
temp<-fuero_comun %>% mutate(cve_ent = str_pad(state_code,width = 2,pad="0"),
                             cve_mun = str_pad(mun_code,width = 3,pad="0")) %>%
  mutate(cve_muni = str_c(cve_ent,cve_mun)) %>%
  dplyr::filter(modalidad=="HOMICIDIOS") 

#homicidios
#Con arma blanca
#Con arma de fuego
#Otros
#dolosos
#culposos

homicidios <- temp %>% group_by(cve_muni) %>%
  dplyr::summarise(homicidios_totales = sum(count), 
                   POB_TOT = mean(population)) %>%
  mutate(tasa_homicidio_d_c = homicidios_totales/POB_TOT*100000) %>%
  mutate(i_amenaza_humana_violecia = ifelse((is.na(tasa_homicidio_d_c))==TRUE,0,tasa_homicidio_d_c))
homicidios$i_amenaza_humana_violecia <- log(homicidios$i_amenaza_humana_violecia)

# Obten el mínimo no infinito
min_homicidio <- homicidios %>% dplyr::select(i_amenaza_humana_violecia) %>% 
  dplyr::filter(is.infinite(i_amenaza_humana_violecia)==FALSE) %>% 
  min()

# sustituye infinito por algo más pequeño
homicidios$i_amenaza_humana_violecia[is.infinite(homicidios$i_amenaza_humana_violecia)]<- min_homicidio - 1
homicidios$i_amenaza_humana_violecia <- scale(homicidios$i_amenaza_humana_violecia)
summary(homicidios$i_amenaza_humana_violecia)
#ggplot(homicidios,aes(i_amenaza_humana_violecia)) + geom_density()

dbWriteTable(con, c("clean",'fuero_comun_municipios'),homicidios, row.names=FALSE)
