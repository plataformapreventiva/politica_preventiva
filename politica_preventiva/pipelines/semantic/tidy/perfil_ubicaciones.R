#!/usr/bin/env Rscript
library(DBI)
library(dbplyr)
library(jsonlite)
library(optparse)
library(tidyverse)
library(stringr)
source("pipelines/semantic/tools/tidy_tools.R")

option_list = list(
  make_option(c("--data_date"), type="character", default="",
              help="data date", metavar="character"),
  make_option(c("--database"), type="character", default="",
              help="database name", metavar="character"),
  make_option(c("--user"), type="character", default="",
              help="database user", metavar="character"),
  make_option(c("--password"), type="character", default="",
              help="password for datbase user", metavar="character"),
  make_option(c("--host"), type="character", default="",
              help="database host name", metavar="character"),
  make_option(c("--pipeline"), type="character", default="",
              help="semantic pipeline task", metavar="character")
  make_option(c("--data_level"), type="character", default="",
              help="data aggregation level", metavar="character")
);

opt_parser <- OptionParser(option_list=option_list);

opt <- tryCatch(
        {
          parse_args(opt_parser);
        },
        error=function(cond) {
            message("Error: Provide database connection arguments appropriately.")
            message(cond)
            print_help(opt_parser)
            return(NA)
        },
        warning=function(cond) {
            message("Warning:")
            message(cond)
            return(NULL)
        },
        finally={
            message("Finished attempting to parse arguments.")
        }
    )

if(length(opt) > 1){

  if (opt$database=="" | opt$user=="" |
      opt$password=="" | opt$host=="" ){
    print_help(opt_parser)
    stop("Database connection arguments are not supplied.n", call.=FALSE)
  }else{
    PGDATABASE <- opt$database
    POSTGRES_PASSWORD <- opt$password
    POSTGRES_USER <- opt$user
    PGHOST <- opt$host
    PGPORT <- "5432"
  }

  con <- DBI::dbConnect(RPostgres::Postgres(),
    host = PGHOST,
    port = PGPORT,
    dbname = PGDATABASE,
    user = POSTGRES_USER,
    password = POSTGRES_PASSWORD
  )

  level <- opt$level
  pipeline_task <- opt$pipeline

  plots_data <- tbl(con, dbplyr::sql(glue::glue("SELECT plot, metadata FROM plots.{pipeline_task} WHERE nivel = '{level}'"))) %>%
                dplyr::collect()

  plots_metadata <- textConnection(dplyr::pull(plots_data, metadata) %>%
                                   gsub("\\n", "", .)) %>%
                    jsonlite::stream_in() %>%
                    dplyr::bind_cols(plots_data, .)

  key <- "variable"
  value <- "valor"

  sql_queries <- purrr::map_chr(1:nrow(plots_metadata),
                                function(x) glue::glue("SELECT {paste(c(key_var, plots_metadata$var_list[[x]]), collapse = ', ')} FROM {plots_metadata$schema[x]}.{plots_metadata$table_name[x]}"))

  tidy_data <- tibble::tibble()

  for(i in 1:nrow(plots_metadata)){
    query <- sql_queries[i]
    data <- tbl(con, dbplyr::sql(query)) %>% dplyr::collect()

    key_var_name <- rlang::sym(key_var)
    key_var_quo <- rlang::quo(!! key_var_name)

    if(level == 'e'){
      key_var <- 'cve_ent'
    } else if(level == 'm') {
        if (plots_metadata$imputacion_estatal){
            key_var <- 'cve_ent'
        } else {
           key_var <- 'cve_muni'
        }
    } else {
      key_var <- NULL
    }

  not_gathered <- c(key_var, "data_date", "actualizacion_sedesol")
    if(plots_metadata$plot[i] == 'piramide_poblacional'){
      data_largo <- data %>%
                    dplyr::mutate(grupo_edad = cut_edad(edad),
                                  sexo = recode(sexo, `1` = 'h', `3` = 'm'),
                                  grupo_pob = paste0(sexo, grupo_edad)) %>%
                    dplyr::group_by(!! key_var_quo, grupo_pob) %>%
                    dplyr::summarise(total_personas = sum(factor)) %>%
                    dplyr::select(!! key_var_quo,
                                  variable = grupo_pob,
                                  valor = total_personas) %>%
                    dplyr::ungroup() %>%
                    dplyr::group_by(!! key_var_quo) %>%
                    dplyr::mutate(element_id = stringr::str_pad(row_number(),
                                                                width = 2,
                                                                pad = '0')) %>%
                    dplyr::ungroup()
    }else{
    data_largo <- tidyr::gather(data, variable, valor, -one_of(not_gathered)) %>%
                  dplyr::arrange(!! key_var_quo) %>%
                  dplyr::group_by(!! key_var_quo) %>%
                  dplyr::mutate(element_id = stringr::str_pad(row_number(),
                                                              width = 2,
                                                              pad = '0')) %>%
                  dplyr::ungroup()
    }

    data_variables <- data_largo %>%
                      dplyr::select(-valor) %>%
                      dplyr::rename(nivel_clave = !! key_var_quo) %>%
                      tidyr::gather(varname, value, -nivel_clave, -element_id) %>%
                      dplyr::mutate(nivel = level,
                                    plot = plots_metadata$plot[i],
                                    vartype = 'x') %>%
                      dplyr::select(nivel, nivel_clave, plot, element_id,
                                    vartype, varname, value)

    data_valores <- data_largo %>%
                      dplyr::select(-variable) %>%
                      dplyr::rename(nivel_clave = !! key_var_quo) %>%
                      tidyr::gather(varname, value, -nivel_clave, -element_id) %>%
                      dplyr::mutate(nivel = level,
                                    plot = plots_metadata$plot[i],
                                    vartype = 'y') %>%
                      dplyr::select(nivel, nivel_clave, plot, element_id,
                                    vartype, varname, value) %>%
                      dplyr::mutate_all(as.character)

  tidy_data <- bind_rows(tidy_data, data_variables, data_valores)
  }

  dplyr::copy_to(con, tidy_data,
                 dbplyr::in_schema('tidy', pipeline_task),
                 temporary = FALSE)
}
