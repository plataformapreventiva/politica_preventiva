#!/usr/bin/env Rscript
library(rlang)
library(tidyverse)

float_columns <- c('num_jueces')
text_columns <- c('nom_ent')

query <- 'SELECT REPLACE(REPLACE(\'entidad_federativa\',\'Ciudad de México\',\'Distrito Federal\'),
        \'Querétaro\',\'Querétaro de Arteaga\') as nom_ent, num_jueces
        FROM raw.inegi_jueces'

make_clean <- function(pipeline_task, con){
  df <- tbl(con, sql(query))
  df %>%
    mutate_at(float_columns, funs(as.numeric(.))) %>%
    mutate_at(text_columns, funs(as.character(.)))
}
