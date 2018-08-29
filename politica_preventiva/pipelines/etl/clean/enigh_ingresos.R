#!/usr/bin/env Rscript
library(rlang)
library(tidyverse)

remove_char <- '&'
int_columns <- c('mes_1','mes_2','mes_3','mes_4','mes_4','mes_5',
                 'mes_6','ing_1','ing_2','ing_3','ing_4','ing_5',
                 'ing_6')
float_columns <- c('ing_tri')
text_columns <- c('clave')

make_clean <- function(pipeline_task, con){
  df <- tbl(con, dbplyr::in_schema('raw', pipeline_task))
  df %>% mutate_if(is.character, funs(ifelse(. == '&', NA, .)), ) %>%
    mutate_at( int_columns, funs(as.integer(.))) %>%
    mutate_at(text_columns, funs(as.character(.)) ) %>%
    mutate_at(float_columns, funs(as.numeric(.)) ) %>%
    mutate(hogar_id = paste(folioviv, foliohog, sep='-')) %>%
    mutate(person_id = paste(folioviv, foliohog, numren, sep='-')) %>%
    mutate(fecha_creacion = substr(data_date, 1, 4))
}
