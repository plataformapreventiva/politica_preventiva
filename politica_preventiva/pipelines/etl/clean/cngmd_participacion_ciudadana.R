#!/usr/bin/env Rscript
library(rlang)
library(tidyverse)

int_columns <- c('nd_nsnn1','tem_pciu')
text_columns <- c('cve_muni')

make_clean <- function(pipeline_task, con){
  
  df <- tbl(con, dbplyr::in_schema('raw', pipeline_task)) %>% 
    mutate(cve_muni = as.character(ubic_geo)) %>% 
    select(cve_muni,nd_nsnn1) %>%
    select(-ubic_geo) %>% distinct() %>% 
    mutate(nd_nsnn1 = ifelse(nd_nsnn1==1 || nd_nsnn1==3 || nd_nsnn1== 6,1/3,0)) %>% 
    group_by(cve_muni) %>% summarise_all(sum,na.rm=TRUE)
  
  df_2 <- tbl(con, dbplyr::in_schema('raw', pipeline_task)) %>% 
    mutate(cve_muni = as.character(ubic_geo)) %>% 
    select(cve_muni,tem_pciu) %>% retrieve_result()
    
  mutate_at(int_columns, funs(as.integer(.))) %>%
    mutate_at(text_columns, funs(as.character(.)) )

    }