#!/usr/bin/env Rscript
library(rlang)
library(tidyverse)

int_columns <- c('secciones_2006','casillas_2006','pan_2006','pri_2006','prd_2006',
                 'pvem_2006','pt_2006','movimientociudadano_2006','nuevaalianza_2006',
                 'coalicionpripvem_2006','coalicionprdptmc_2006', 'coalicionprdpt_2006',
                 'coalicionprdmc_2006','coalicionptmc_2006','noregistrados_2006',      
                 'nulos_2006','total_2006','listanominal_2006','participacion_2006',      
                 'secciones_2012','casillas_2012','pan_2012',                
                 'alianzapormexico_2012','porelbiendetodos_2012','nuevaalianza_2012',
                 'alternativa_2012','noregistrados_2012','validos_2012','nulos_2012',
                 'total_2012','listanominal_2012','participacion_2012')
float_columns <- c('participacion_2006','participacion_2012')
text_columns <- c('nom_ent','observaciones_2006')

make_clean <- function(pipeline_task, con){
  df <- tbl(con, dbplyr::in_schema('raw', pipeline_task)) %>% 
    select(-c(entidadfederativa,data_date,actualizacion_sedesol)) %>%
    mutate_if(is.character,str_replace_all, pattern = "%", replacement = "") %>%
    mutate_at(float_columns, funs(as.numeric(.))) %>%
    mutate_at(int_columns, funs(as.integer(.))) %>% 
    mutate_at(float_columns, funs((.)/100))
  
  corrupcion_partidos <- data.frame(nom_ent=c("Aguascalientes","Baja California",
                                              "Baja California Sur","Campeche",
                                              "Coahuila de Zaragoza","Colima","Chiapas",
                                              "Chihuahua","Distrito Federal","Durango",
                                              "Guanajuato","Guerrero","Hidalgo",
                                              "Jalisco","México","Michoacán de Ocampo",
                                              "Morelos","Nayarit","Nuevo León","Oaxaca",
                                              "Puebla","Querétaro de Arteaga",
                                              "Quintana Roo","San Luis Potosí",
                                              "Sinaloa","Sonora","Tabasco","Tamaulipas",
                                              "Tlaxcala","Veracruz de Ignacio de la Llave",
                                              "Yucatán","Zacatecas"),df) %>%
    mutate_at(text_columns, funs(as.character(.)))
}

