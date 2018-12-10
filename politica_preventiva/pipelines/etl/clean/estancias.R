#!/usr/bin/env Rscript
library(rlang)
library(tidyverse)


query <- "SELECT lpad(cve_munc::TEXT,5,'0') as cve_muni,
            lpad(cve_edo::TEXT,2,'0') as cve_ent, * FROM raw.estancias"

make_clean <- function(pipeline_task, con){
  df <- tbl(con, sql(query))
  df }
