#!/usr/bin/env Rscript
library(rlang)
library(tidyverse)

int_columns <- c('legislacion_reconoce_matrimonio_igualitario')
text_columns <- c('nom_ent','fundamento_articulo','cita', 'fuente', 'link')

make_clean <- function(pipeline_task, con){
  df <- tbl(con, dbplyr::in_schema('raw', pipeline_task)) %>% 
    mutate(nom_ent=estado) %>% 
    select(-c(data_date,actualizacion_sedesol,estado)) %>%
    mutate_at(text_columns, funs(as.character(.))) %>%
    mutate_at(int_columns, funs(as.integer(.)))
}