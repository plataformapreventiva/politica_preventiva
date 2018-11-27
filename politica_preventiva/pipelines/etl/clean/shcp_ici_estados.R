#!/usr/bin/env Rscript
library(rlang)
library(tidyverse)

int_columns <- c('ciclo','trimestre','id_tipo_valoracion_ici')
text_columns <- c('id_entidad_federativa','entidad_federativa','tipo_valoracion_ici','actualizacion_sedesol','data_date')
float_columns <- c('valor_ici')

query <- 'SELECT *, LPAD(id_entidad_federativa::text, 2, \'0\') as cve_ent FROM raw.shcp_ici_estados'

make_clean <- function(pipeline_task, con){
  df <- tbl(con, sql(query)) %>%
    mutate_at(int_columns, funs(as.integer(.))) %>%
    mutate_at(float_columns, funs(as.numeric(.))) %>%
    mutate_at(text_columns, funs(as.character(.)))
}