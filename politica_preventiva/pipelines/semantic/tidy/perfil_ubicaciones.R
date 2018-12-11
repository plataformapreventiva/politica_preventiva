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
    host=PGHOST,
    port=PGPORT,
    dbname=PGDATABASE,
    user=POSTGRES_USER,
    password=POSTGRES_PASSWORD
  )

  level <- opt$level
  pipeline_task <- opt$pipeline

  plots_data <- tbl(con, dbplyr::sql(glue::glue("SELECT plot, plot_type, metadata \
                                                FROM plots.{pipeline_task} \
                                                WHERE nivel = '{level}'"))) %>%
                dplyr::collect() %>%
                dplyr::filter(!grepl('^programas_', plot),
                              !grepl('^distribucion_', plot),
                              !grepl('^poblacion_atendida', plot),
                              !grepl('^capacidad_institucional', plot))

  plots_metadata <- textConnection(dplyr::pull(plots_data, metadata) %>%
                                   gsub("\\n", "", .)) %>%
                    jsonlite::stream_in() %>%
                    dplyr::bind_cols(plots_data, .)

  key_var <- 'cve_muni'
  sql_queries <- purrr::map_chr(1:nrow(plots_metadata),
                                function(x) glue::glue("SELECT {paste(c(key_var,
                                                       plots_metadata$vars[[x]]),
                                collapse = ', ')} FROM {plots_metadata$schema[x]}.{plots_metadata$table_name[x]}"))

  query <- c("DROP TABLE IF EXISTS tidy.perfil_ubicaciones;
              CREATE TABLE tidy.perfil_ubicaciones(
                nivel TEXT,
                nivel_clave TEXT,
                plot TEXT,
                element_id TEXT,
                values JSONB
             );")

  DBI::dbGetQuery(con, query)
  dbCommit(con)
  dbDisconnect(con)

  tidy_data <- tibble::tibble()
  for(i in 1:nrow(plots_metadata)){
    query <- sql_queries[i]
    con <- DBI::dbConnect(RPostgres::Postgres(),
                          host=PGHOST,
                          port=PGPORT,
                          dbname=PGDATABASE,
                          user=POSTGRES_USER,
                          password=POSTGRES_PASSWORD
    )
    print('Opened connection')
    data <- tbl(con, dbplyr::sql(query)) %>% dplyr::collect()

    key_var_name <- rlang::sym(key_var)
    key_var_quo <- rlang::quo(!! key_var_name)

    not_gathered <- c(key_var)
    print(glue::glue('Query number {i}'))
    if(plots_metadata$plot[i] == 'piramide_poblacional'){
      data_largo <- data %>%
                    dplyr::mutate(grupo_edad = cut_edad(edad),
                                  sexo = recode(sexo, `1` = 'h', `3` = 'm'),
                                  grupo_pob = paste0(sexo, grupo_edad)) %>%
                    dplyr::group_by(!! key_var_quo, grupo_pob) %>%
                    dplyr::summarise(total_personas = sum(poblacion_mun)) %>%
                    dplyr::select(nivel_clave = !! key_var_quo,
                                  variable = grupo_pob,
                                  valor = total_personas) %>%
                    dplyr::ungroup()
    }else{
    data_largo <- tidyr::gather(data, variable, valor, -one_of(not_gathered)) %>%
                  dplyr::rename(nivel_clave = !! key_var_quo)
    }
    if(plots_metadata$plot_type[i] == 'map'){
        # Agarrar una variable adicional, e.g. localidad o municipio o clave de
        # algo, y agrupar con ella como element id
        # Una idea:
        # Acordarme de sacar del if todo lo que tenga sentido sacar


    # if(level == 'e'){
    #  key_var <- 'cve_ent'
    #} else if(level == 'm') {
    #    if (plots_metadata$imputacion_estatal){
    #        key_var <- 'cve_ent'
    #    } else {
    #       key_var <- 'cve_muni'
    #    }
    #} else {
    #  key_var <- NULL
    #}
        print('This query is a map')
        data_to_plot <- tibble()
    } else {
        data_to_plot <- data_largo %>%
                        dplyr::group_by(nivel_clave) %>%
                        dplyr::arrange(variable) %>%
                        dplyr::mutate(element_id = stringr::str_pad(row_number(),
                                                                    width = 2,
                                                                    pad = '0')) %>%
                        dplyr::ungroup() %>%
                        tidyr::gather(varname, value, -nivel_clave, -element_id) %>%
                        dplyr::mutate(vartype = dplyr::recode(varname,
                                                              variable='x',
                                                              valor='y')) %>%
                        arrange(nivel_clave, element_id) %>%
                                      nivel = level,
                                      plot = plots_metadata$plot[i]) %>%
                       dplyr::select(nivel, nivel_clave, plot, element_id, values)
    }
  tidy_data <- data_to_plot %>%
               dplyr::select(nivel, nivel_clave, plot,
                             element_id, values)

  dplyr::copy_to(dest=con,
                 df=tidy_data,
                 name=dbplyr::in_schema('tidy',
                                        'perfil_ubicaciones'),
                 overwrite=FALSE)
  print(glue::glue('Wrote table {i}'))
  RPostgresql::dbDisconnect(con)
  print('Closed connection')
  }
}
