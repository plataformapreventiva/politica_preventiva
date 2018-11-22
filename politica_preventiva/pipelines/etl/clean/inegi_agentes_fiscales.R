#!/usr/bin/env Rscript
library(rlang)
library(tidyverse)

float_columns <- c('num_agentes_fiscales')
text_columns <- c('nom_ent')
int_columns <- c('anio')

query <- 'SELECT CASE entidad_federativa WHEN \'Ciudad de México\'
                                          THEN \'Distrito Federal\'
                                          WHEN \'Querétaro\'
                                          THEN \'Querétaro de Arteaga\'
                                          ELSE entidad_federativa
                  END as nom_ent, 
          num_agentes_fiscales,
          anio
          FROM raw.inegi_agentes_fiscales'

make_clean <- function(pipeline_task, con){
  df <- tbl(con, sql(query))
  df %>%
    mutate_at(float_columns, funs(as.numeric(.))) %>%
    mutate_at(text_columns, funs(as.character(.))) %>%
    mutate_at(int_columns, funs(as.integer(.)))
}