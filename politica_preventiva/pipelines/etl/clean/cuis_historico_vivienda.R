#!/usr/bin/env Rscript
library(rlang)
library(dplyr)
library(tidyr)

int_columns <- c('c_tipo_viv', 'tot_per_viv',
                 'tot_hog', 'tot_per', 'per_gasto',
                 'per_alim')

text_columns <- c('llave_hogar_h', 'fch_creacion',
                  'usr_creacion', 'csc_hogar')

replace_na <- c('')

# The ETL pipeline will call this function to run UpdateCleanDB task

make_clean <- function(pipeline_task, con){
  df <- tbl(con, dbplyr::in_schema('raw', pipeline_task))
  df %>%
      mutate_at(vars(one_of(int_columns)), as.integer) %>%
      mutate_at(vars(one_of(replace_na)), funs(ifelse(is.na(.), 2, .))) %>%
      mutate(hogar_id = llave_hogar_h) %>%
      mutate(anio = substr(fch_creacion,7,8)) %>%
      mutate(anio = as.integer(anio))
}
