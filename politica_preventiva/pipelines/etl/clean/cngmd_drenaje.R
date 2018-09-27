#!/usr/bin/env Rscript
library(rlang)
library(tidyverse)

float_columns <- c('cobertura_dren')
text_columns <- c('cve_muni')

make_clean <- function(pipeline_task, con){
  df <- tbl(con, dbplyr::in_schema('raw', pipeline_task)) %>%
    mutate(cve_muni = aps_folio,
           cobertura_dren=(sd_dren_cab+sd_dren_resto)/2)  %>%
    select(cve_muni,cobertura_dren) %>%
    mutate_at(text_columns, funs(as.character(.)) ) %>%
    mutate_at(float_columns, funs(as.numeric(.)) ) %>%
    mutate(cobertura_dren=cobertura_dren/100)
}
