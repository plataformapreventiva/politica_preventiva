#!/usr/bin/env Rscript
library(rlang)
library(tidyverse)

query <- "SELECT LPAD(cve_ent::TEXT, 2, '0') || LPAD(cve_mun::TEXT, 3, '0') as cve_muni,
            * FROM raw.denue"

make_clean <- function(pipeline_task, con){
  df <- tbl(con, sql(query))
}
