#!/usr/bin/env Rscript
library(rlang)
library(tidyverse)

int_columns <- c('poblacion_18_mas_2011','poblacion_18_mas_2013', 'poblacion_18_mas_2015',
                 'muy_frecuente_absoluto_2011','frecuente_absoluto_2011', 
                 'poco_frecuente_absoluto_2011','nunca_absoluto_2011',
                 'muy_frecuente_absoluto_2013','frecuente_absoluto_2013',
                 'poco_frecuente_absoluto_2013','nunca_absoluto_2013', 
                 'muy_frecuente_absoluto_2015','frecuente_absoluto_2015',
                 'poco_frecuente_absoluto_2015','nunca_absoluto_2015')
float_columns <- c('muy_frecuente_realtivo_2011','frecuente_relativo_2011',
                   'poco_frecuente_relativo_2011','nunca_relativo_2011',
                   'muy_frecuente_realtivo_2013','frecuente_relativo_2013',
                   'poco_frecuente_relativo_2013','nunca_relativo_2013',
                   'muy_frecuente_realtivo_2015', 'frecuente_relativo_2015',
                   'poco_frecuente_relativo_2015','nunca_relativo_2015')
text_columns <- c('nom_ent')
remove_char <- c(',','NA','0*','0.0*')

make_clean <- function(pipeline_task, con){
  df <- tbl(con, dbplyr::in_schema('raw', pipeline_task)) %>% 
    mutate_if(is.character, funs(ifelse(. == ',', NA, .)), ) %>%
    mutate_if(is.character, funs(ifelse(. == 'NA', NA, .)), ) %>%
    mutate_if(is.character, funs(ifelse(. == '0*', '0', .)), ) %>%
    mutate_if(is.character, funs(ifelse(. == '0.0*', '0', .)), ) %>%
    mutate(nom_ent=entidad_federativa) %>%
    select(-c(data_date,actualizacion_sedesol,entidad_federativa)) %>%
    mutate_at(text_columns, funs(as.character(.))) %>%
    mutate_at(float_columns, funs(as.numeric(.))) %>%
    mutate_at(int_columns, funs(as.integer(.)))
}