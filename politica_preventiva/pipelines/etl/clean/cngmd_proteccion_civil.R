#!/usr/bin/env Rscript
library(rlang)
library(tidyverse)

int_columns <- c('nd_spnn1','nd_nsnn4')
text_columns <- c('cve_muni')

make_clean <- function(pipeline_task, con){
  df <- tbl(con, dbplyr::in_schema('raw', pipeline_task)) %>%
    mutate(cve_muni = as.character(ubic_geo)) %>%
    select(cve_muni, nd_spnn1,nd_nsnn4) %>%
    mutate_at(int_columns, funs(as.integer(.))) %>%
    mutate_at(text_columns, funs(as.character(.)))
}