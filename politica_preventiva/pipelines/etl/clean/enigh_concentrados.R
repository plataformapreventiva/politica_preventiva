#!/usr/bin/env Rscript
library(rlang)
library(tidyverse)


make_clean <- function(pipeline_task, con){
  df <- tbl(con, dbplyr::in_schema('raw', pipeline_task))
  df %>% mutate(hogar_id = paste(folioviv, foliohog, sep='-')) %>%
    mutate(fecha_creacion = substr(data_date, 1, 4))
}
