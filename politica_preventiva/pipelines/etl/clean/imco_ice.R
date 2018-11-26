#!/usr/bin/env Rscript
library(rlang)
library(tidyverse)

float_columns <- c('general','derecho','ambiente','sociedad','politico',
                   'gobiernos','factores','economia','precursores','relaciones',
                   'innovacion')
text_columns <- c('cve_ent','entidad','actualizacion_sedesol','data_date')
int_columns <- c('anio')

query <- 'SELECT *, LPAD(clavedeentidad::text, 2, \'0\') as cve_ent FROM raw.imco_ice'

make_clean <- function(pipeline_task, con){
  df <- tbl(con, sql(query)) %>%
    mutate_at(text_columns, funs(as.character(.))) %>%
    mutate_at(int_columns, funs(as.integer(.))) %>%
    mutate_at(float_columns, funs(as.numeric(.)))
}