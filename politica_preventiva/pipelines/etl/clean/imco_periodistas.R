#!/usr/bin/env Rscript
library(rlang)
library(tidyverse)

int_columns <- c('agresiones_a_periodistas_2008','agresiones_a_periodistas_2009',
'agresiones_a_periodistas_2010','agresiones_a_periodistas_2011','agresiones_a_periodistas_2012',
'agresiones_a_periodistas_2013', 'agresiones_a_periodistas_2014','agresiones_a_periodistas_2015')
text_columns <- c('nom_ent')

make_clean <- function(pipeline_task, con){
  df <- tbl(con, dbplyr::in_schema('raw', pipeline_task)) %>%
    rename(nom_ent=entidad_federativa) %>%
    select(-c(data_date,actualizacion_sedesol)) %>%
    mutate_at(text_columns, funs(as.character(.)) ) %>%
    mutate_at(int_columns, funs(as.integer(.)) )
}