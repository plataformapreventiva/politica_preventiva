#!/usr/bin/env Rscript
library(rlang)
library(tidyverse)

int_columns <- c('inf_publ','mec_tran')
text_columns <- c('cve_muni','ubic_geo')

make_clean <- function(pipeline_task, con){
  
  mectran <- tbl(con, dbplyr::in_schema('raw', pipeline_task)) %>% 
    select(ubic_geo,mec_tran) %>% 
    mutate(cve_muni = as.character(ubic_geo)) %>% 
    select(-ubic_geo) %>% distinct() %>% 
    mutate(mec_tran = ifelse(mec_tran==1 || mec_tran==3 || mec_tran== 6,1/3,0)) %>% 
    group_by(cve_muni) %>% summarise_all(sum,na.rm=TRUE)
  
  infpubl <- tbl(con, dbplyr::in_schema('raw', pipeline_task)) %>% 
    select(ubic_geo,inf_publ) %>% 
    mutate(cve_muni = as.character(ubic_geo)) %>% 
    select(-ubic_geo) %>% distinct() %>% 
    mutate(inf_publ = ifelse(inf_publ==5 || inf_publ==22 || inf_publ==23 || inf_publ==42 || inf_publ==44,1/5,0)) %>%
    group_by(cve_muni) %>% summarise_all(sum,na.rm=TRUE)
  
  cngmd_transparencia <- left_join(mectran,infpubl,by='cve_muni')
  
}
