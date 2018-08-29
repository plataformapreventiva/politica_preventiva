#!/usr/bin/env Rscript
library(rlang)
library(dplyr)
library(tidyr)

int_columns <- c('c_con_res', 'c_cd_parentesco',
                 'reside', 'padre', 'madre',
                 'c_cd_edo_civil', 'edad',
                 'val_nb_renapo', 'num_per',
                 'conyuge')

text_columns <- c('llave_hogar_h', 'fch_creacion',
                  'usr_creacion')

replace_na <- c('')

# The ETL pipeline will call this function to run UpdateCleanDB task

make_clean <- function(pipeline_task, con){
  df <- tbl(con, dbplyr::in_schema('raw', pipeline_task))
  df %>%
      mutate_at(vars(one_of(int_columns)), as.integer) %>%
      mutate_at(vars(one_of(replace_na)), funs(ifelse(is.na(.), 2, .))) %>%
      mutate(hogar_id = llave_hogar_h) %>%
      mutate(person_id = paste(llave_hogar_h, c_integrante, sep='-')) %>%
      mutate(anio = substr(fch_creacion, 7, 10)) %>%
      mutate(anio = as.integer(anio))
}
