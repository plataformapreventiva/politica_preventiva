#!/usr/bin/env Rscript
library(rlang)
library(tidyverse)

int_columns <- c('acc_info_est','acc_info_est__1')
text_columns <- c('cve_ent')

make_clean <- function(pipeline_task, con){
  df <- tbl(con, dbplyr::in_schema('raw', pipeline_task)) %>% 
    mutate(cve_ent=entidad) %>%
    select(-c(data_date,actualizacion_sedesol,entidad)) %>% distinct() %>%
    mutate_at(text_columns, funs(as.character(.))) %>%
    mutate_at(int_columns, funs(as.integer(.)))
}