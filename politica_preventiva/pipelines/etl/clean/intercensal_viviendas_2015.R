#!/usr/bin/env Rscript
library(rlang)
library(tidyverse)

text_columns <- c('cve_muni')
float_columns <- c('viviendas_mun','jefatura_fem','unipersonales')

query <- 'SELECT LPAD(ent::text, 2, \'0\') || LPAD(mun::text, 3, \'0\') as cve_muni,
                                    sum (factor) as viviendas_mun,
                                    sum(CASE 
                                    WHEN CAST(jefe_sexo as integer) = 3
                                    THEN factor 
                                    ELSE 0 
                                    END) as jefatura_fem,
                                    sum(CASE 
                                    WHEN CAST(tipohog as integer) = 5
                                    THEN factor 
                                    ELSE 0 
                                    END) as unipersonales
                                    FROM raw.intercensal_viviendas_2015
                                    GROUP BY cve_muni'

make_clean <- function(pipeline_task, con){
  df <- tbl(con, sql(query))
  df %>%
    mutate_at(float_columns, funs(as.numeric(.))) %>%
    mutate_at(text_columns, funs(as.character(.)))
}