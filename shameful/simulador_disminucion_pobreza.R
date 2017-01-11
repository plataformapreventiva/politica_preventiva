####
# A este simulador le pasas un porcentaje de disminución de pobreza extrema
# y te daun vector de personas por carencia social.
#
# Error - falta hacer bien la suma
####

rm(list=ls())
suppressPackageStartupMessages({
  library(tidyverse)
  library(doBy)
  library(reshape)
  library(data.table)
  library(stats)
})

disminucion_extrema <- .10

####################
# function
####################
base <- read_csv("../data/coneval/programas_calculo/R_2014/Base final/pobreza_14.csv")

simulacion_disminucion <- function(estado,disminucion_extrema){
  # filtramos solo pobreza extrema
  base <- filter(base, pobreza_e>0, ent == estado)
  
  # obtenemos meta de personas
  disminucion_meta <- sum(base$factor_hog[base["pobreza_e"]==1], na.rm=TRUE)

  # vector preseleccionado de preferencias
  temp <- arrange(base,i_privacion, desc(ic_rezedu) ,desc(ic_asalud),desc(ic_segsoc) 
                  ,desc(ic_sbv) , desc(ic_cv) ,desc(ic_ali))
  
  # Ve cuántas personas tenemos que reducir de pobreza
  i <- 0
  while(sum(temp$factor_hog[1:i],na.rm = TRUE) < disminucion_meta){
    i <- i + 1
  }

  folios_disminucion <- temp$folioviv[1:i]
  base_disminucion <- temp[temp$folioviv %in% folios_disminucion,]
  total_personas <- sum(temp[temp$folioviv %in% folios_disminucion,]["factor_hog"])
  
  # tenemos que sumar a las personas que tienen 3 carencias y seguir con 4 y 5.
  ic_rezedu <- sum(base_disminucion$factor_hog[(base_disminucion["ic_rezedu"]==1) & 
                                                 (base_disminucion["i_privacion"]==3)], na.rm=TRUE)
  ic_asalud <- sum(base_disminucion$factor_hog[(base_disminucion["ic_asalud"]==1) & 
                                                 (base_disminucion["ic_rezedu"]==0) & 
                                                 (base_disminucion["i_privacion"]==3)], na.rm=TRUE)
  ic_segsoc <- sum(base_disminucion$factor_hog[(base_disminucion["ic_segsoc"]==1) & 
                                                 (base_disminucion["ic_asalud"]==0) & 
                                                 (base_disminucion["ic_rezedu"]==0) & 
                                                 (base_disminucion["i_privacion"]==3)], na.rm=TRUE)
  ic_sbv <- sum(base_disminucion$factor_hog[(base_disminucion["ic_sbv"]==1) &
                                                 (base_disminucion["ic_segsoc"]==0) & 
                                                 (base_disminucion["ic_asalud"]==0) & 
                                                 (base_disminucion["ic_rezedu"]==0) & 
                                                 (base_disminucion["i_privacion"]==3)], na.rm=TRUE)
  ic_cv <- sum(base_disminucion$factor_hog[(base_disminucion["ic_cv"]==1) & 
                                             (base_disminucion["ic_sbv"]==0) &
                                                 (base_disminucion["ic_segsoc"]==0) & 
                                                 (base_disminucion["ic_asalud"]==0) & 
                                                 (base_disminucion["ic_rezedu"]==0) & 
                                                 (base_disminucion["i_privacion"]==3)], na.rm=TRUE)
  ic_ali <- sum(base_disminucion$factor_hog[(base_disminucion["ic_ali"]==1) & 
                                              (base_disminucion["ic_cv"]==0) & 
                                             (base_disminucion["ic_sbv"]==0) &
                                                 (base_disminucion["ic_segsoc"]==0) & 
                                                 (base_disminucion["ic_asalud"]==0) & 
                                                 (base_disminucion["ic_rezedu"]==0) & 
                                                 (base_disminucion["i_privacion"]==3)], na.rm=TRUE)
  

  return_list <- c(total_personas,ic_rezedu, ic_asalud, ic_segsoc, ic_sbv, ic_cv, ic_ali)

return(return_list)
}

temp <- simulacion_disminucion("01",.1)

