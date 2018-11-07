#!/usr/bin/env Rscript
library(rlang)
library(tidyverse)

float_columns <- c('basura_porcent_cab')
text_columns <- c('cve_muni')

make_clean <- function(pipeline_task, con){
  df <- tbl(con, dbplyr::in_schema('raw', pipeline_task)) %>%
    mutate(cve_muni = as.character(rsu_folio)) %>% 
    select(cve_muni,basura_porcent_cab) %>%
    mutate_at(text_columns, funs(as.character(.)) ) %>%
    mutate_at(float_columns, funs(as.numeric(.)) ) %>% 
    mutate(basura_porcent_cab=basura_porcent_cab/100)
}