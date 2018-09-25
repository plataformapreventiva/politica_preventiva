#!/usr/bin/env Rscript
library(rlang)
library(tidyverse)

text_columns <- c('cve_muni')
float_columns <- c('n_usuarios')

query <- 'SELECT LPAD(cveinegi::text, 2, \'0\') || LPAD(cvemun::text, 3, \'0\') as cve_muni, sum(num) as n_usuarios
  FROM raw.cfe
  WHERE tipo like \'Usuarios\'
  GROUP BY cve_muni'

make_clean <- function(pipeline_task, con){
  df <- tbl(con, sql(query))
  df %>%
    mutate_at(float_columns, funs(as.numeric(.))) %>%
    mutate_at(text_columns, funs(as.character(.)))
}
