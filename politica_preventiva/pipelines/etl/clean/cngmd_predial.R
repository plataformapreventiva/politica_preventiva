#!/usr/bin/env Rscript
library(rlang)
library(tidyverse)

remove_char <- c('NSS','"NA"')
int_columns <- c('aut_cobr')
text_columns <- c('cve_muni')
float_columns <- c('totalpo1')

make_clean <- function(pipeline_task, con){
  df <- tbl(con, dbplyr::in_schema('raw', pipeline_task)) %>%
    mutate(cve_muni=ubic_geo) %>%
    select(cve_muni,aut_cobr,totalpo1) %>%
    mutate_if(is.character, funs(ifelse(. == 'NSS', NA, .)),) %>%
    mutate_if(is.character, funs(ifelse(. == '"NA"', NA, .)),) %>%
    mutate_at(int_columns, funs(as.integer(.))) %>%
    mutate_at(float_columns, funs(as.numeric(.))) %>%
    mutate_at(text_columns, funs(as.character(.))) %>%
    mutate(totalpo1=totalpo1/100)
}