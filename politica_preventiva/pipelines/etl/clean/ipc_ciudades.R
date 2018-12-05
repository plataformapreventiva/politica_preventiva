#!/usr/bin/env Rscript
make_clean <- function(pipeline_task, con){
    dplyr::tbl(con, dbplyr::in_schema('raw', pipeline_task)) %>%
      dplyr::mutate(nom_cd = stringr::str_trim(nom_cd, side='both'))
}

