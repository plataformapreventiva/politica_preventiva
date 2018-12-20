#!/usr/bin/env Rscript
library(rlang)
library(tidyverse)


query <- 'select data_date as fecha, * from raw.coneval_municipios'

make_clean <- function(pipeline_task, con){
  df <- tbl(con, sql(query))
}
