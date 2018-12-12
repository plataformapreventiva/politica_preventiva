#!/usr/bin/env Rscript
library(rlang)
library(tidyverse)

float_columns <- c('complejidad_2004','complejidad_2005','complejidad_2006','complejidad_2007',
                   'complejidad_2008','complejidad_2009','complejidad_2010','complejidad_2011',
                   'complejidad_2012','complejidad_2013','complejidad_2014')
text_columns <- c('cve_muni','nom_muni','cve_ent')

make_clean <- function(pipeline_task, con){
  df <- tbl(con, dbplyr::in_schema('raw', pipeline_task)) %>%
    mutate_at(text_columns, funs(as.character(.))) %>%
    mutate_at(float_columns, funs(as.numeric(.)))
}
