#!/usr/bin/env Rscript
library(rlang)
library(tidyverse)

int_columns <- c('nd_nsnn1')
text_columns <- c('cve_muni')

make_clean <- function(pipeline_task, con){
  
  df <- tbl(con, dbplyr::in_schema('raw', pipeline_task)) %>% 
    mutate(cve_muni = as.character(ubic_geo)) %>% 
    select(cve_muni,nd_nsnn1) %>% mutate(nd_nsnn1 = ifelse(nd_nsnn1==1,1,0)) %>% distinct() %>%
    group_by(cve_muni) %>% summarise_all(sum,na.rm=TRUE) %>%
    mutate_at(int_columns, funs(as.integer(.))) %>%
    mutate_at(text_columns, funs(as.character(.)) )
    }