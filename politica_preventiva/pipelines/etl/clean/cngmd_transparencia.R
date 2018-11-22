#!/usr/bin/env Rscript
library(rlang)
library(tidyverse)

int_columns <- c('inf_publ','mec_tran')
text_columns <- c('cve_muni','ubic_geo')

make_clean <- function(pipeline_task, con){
  
  cngmd_transparencia <- tbl(con, dbplyr::in_schema('raw', pipeline_task)) %>%
    select(ubic_geo,mec_tran,inf_publ) %>% distinct() %>%
    mutate(cve_muni = as.character(ubic_geo),
           mec_tran = ifelse(mec_tran==1 || mec_tran==3 || mec_tran== 6,1/3,0),
           inf_publ = ifelse(inf_publ==5 || inf_publ==22 || inf_publ==23 || inf_publ==42 || inf_publ==44,1/5,0)) %>% 
    select(-ubic_geo) %>%
    group_by(cve_muni) %>%
    summarise_all(sum) %>%
    mutate_at(int_columns, funs(as.integer(.))) %>%
    mutate_at(text_columns, funs(as.character(.)))
}