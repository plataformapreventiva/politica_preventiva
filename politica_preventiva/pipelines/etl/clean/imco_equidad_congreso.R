#!/usr/bin/env Rscript
library(rlang)
library(tidyverse)

int_columns <- c('numero_diputados_julio_2015',     
'diputados_2015','diputadas_2015','numero_diputados_julio_2016',
'diputados_2016','diputadas_2016')
float_columns <- c('participacion_mujeres_julio_2014','porcen_diputados_2015',
                   'porcen_diputadas_2015','porcen_diputados_2016','porcen_diputadas_2016')
text_columns <- c('nom_ent')

make_clean <- function(pipeline_task, con){
  df <- tbl(con, dbplyr::in_schema('raw', pipeline_task)) %>% 
    mutate(nom_ent=entidad_federativa) %>%
    select(-c(data_date,actualizacion_sedesol,entidad_federativa)) %>%
    mutate_at(text_columns, funs(as.character(.)) ) %>%
    mutate_at(float_columns, funs(as.numeric(.)) ) %>%
    mutate_at(int_columns, funs(as.integer(.)) ) %>%
    mutate(participacion_mujeres_julio_2014=participacion_mujeres_julio_2014/100)
}