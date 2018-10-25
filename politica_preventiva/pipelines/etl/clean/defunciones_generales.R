#!/usr/bin/env Rscript
library(rlang)
library(tidyverse)

int_columns <- c('infantil','materna')
text_columns <- c('nom_ent')

query <- 'SELECT LPAD(ent_ocurr::text, 2, \'0\') || LPAD(mun_ocurr::text, 3, \'0\') as cve_muni, 
          sum(CASE 
              WHEN CAST(edad as integer) < 4000
                  THEN 1 
                  ELSE 0 
              END) as infantil, 
          sum(CASE 
              WHEN causa_def LIKE \'O%\'
                  THEN 1 
                  ELSE 0 
              END) as materna
          FROM raw.defunciones_generales
          GROUP BY cve_muni'

make_clean <- function(pipeline_task, con){
  df <- tbl(con, sql(query))
  df %>%
    mutate_at(int_columns, funs(as.integer(.))) %>%
    mutate_at(text_columns, funs(as.character(.)))
}