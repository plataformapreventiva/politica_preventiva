#!/usr/bin/env Rscript
library(rlang)
library(tidyverse)

text_columns <- c('nom_ent','leyocodigo','articulos','listanominalopadron','fechadeconsulta',
                  'fechadeultimaversion','mismaleydelicu','fuente','fuentesecundaria',
                  'gobernador_3','diputados_3','ayuntamiento_3')
float_columns <- c('gobernador','gobernador_2','diputados_2','ayuntamiento_2')

make_clean <- function(pipeline_task, con){
  df <- tbl(con, dbplyr::in_schema('raw', pipeline_task)) %>% 
    mutate(nom_ent=entidad) %>%
    mutate_if(is.character, funs(ifelse(. == 'NA', NA, .)), ) %>%
    select(-c(data_date,actualizacion_sedesol,entidad)) %>% distinct() %>%
    mutate_at(text_columns, funs(as.character(.))) %>%
    mutate_at(float_columns, funs(as.numeric(.)))
}