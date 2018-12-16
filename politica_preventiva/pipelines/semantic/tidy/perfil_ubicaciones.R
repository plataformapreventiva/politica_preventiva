#!/usr/bin/env Rscript
library(DBI)
library(stringr)
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
              help="semantic pipeline task", metavar="character"),
  make_option(c("--extra_parameters"), type="character", default="",
              help="Extra parameters", metavar="character")
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

  pipeline_task <- opt$pipeline

  query <- c("DROP TABLE IF EXISTS tidy.perfil_ubicaciones;")
  query_2 <- c("CREATE TABLE IF NOT EXISTS  temp_perfil_ubicaciones_t2 (
                    nivel TEXT,
                    nivel_clave TEXT,
                    plot TEXT,
                    values TEXT,
                    metadata TEXT);")
  DBI::dbGetQuery(con, query)
  DBI::dbGetQuery(con, query_2)

  for(level in as.list(strsplit(opt$extra_parameters, ",")[[1]])){
    if (level=='m'){
      key_var <- 'cve_muni'
      n <- 5
    } else{
      key_var <- 'cve_ent'
      n <-2
    }

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
    plots_metadata_d <- plots_metadata %>%
      rowwise %>%
      mutate(metadata = toJSON(list(plot_type=plot_type,
                                  metadata=metadata,
                                  title=title,
                                  palette=palette,
                                  subtext=subtext),auto_unbox=T)) %>%
      dplyr::select(plot, metadata)

    sql_queries <- purrr::map_chr(1:nrow(plots_metadata),
                                  function(x) glue::glue("SELECT {paste(c(key_var,
                                                         plots_metadata$vars[[x]]),
                                  collapse = ', ')} FROM {plots_metadata$schema[x]}.{plots_metadata$table_name[x]}"))


    tidy_data <- tibble::tibble()
    concat <- tibble::tibble()

    for(i in 1:nrow(plots_metadata)){
      con <- DBI::dbConnect(RPostgres::Postgres(),
        host=PGHOST,
        port=PGPORT,
        dbname=PGDATABASE,
        user=POSTGRES_USER,
        password=POSTGRES_PASSWORD)
      query <- sql_queries[i]
      print(query)
      print('Opened connection')
      data <- tbl(con, dbplyr::sql(query)) %>% dplyr::collect()
      key_var_name <- rlang::sym(key_var)
      key_var_quo <- rlang::quo(!! key_var_name)

      not_gathered <- c(key_var)
      print(glue::glue('Query number {i}'))

      data_largo <- tidyr::gather(data, variable, valor, -one_of(not_gathered)) %>%
                    dplyr::rename(nivel_clave = !! key_var_quo)

      data_to_plot <- data_largo %>%
         dplyr::mutate(nivel_clave = str_pad(nivel_clave,n,"left", '0')) %>%
         dplyr::group_by(nivel_clave) %>%
         dplyr::arrange(variable) %>%
         dplyr::mutate(element_id = stringr::str_pad(row_number(),
                                                     width = 2,
                                                     pad = '0')) %>%
         dplyr::ungroup() %>%
         rowwise %>%
         dplyr::mutate(values = str_c('"',variable,'":{"valor":"',valor,'"}'),
                 nivel = level,
                 plot = plots_metadata$plot[i]) %>%
         drop_na(values) %>%
         dplyr::select(-element_id, -valor, -variable) %>%
         dplyr::group_by(nivel, nivel_clave, plot) %>%
         dplyr::summarise(values=paste(values, collapse=', ')) %>%
         dplyr::left_join(plots_metadata_d) %>%
         dplyr::mutate(values = str_c('"',plot,'":{"values":{',values,
                                   '},"info":',metadata,'}')) %>%
         dplyr::select(-metadata)

      RPostgres::dbWriteTable(conn=con,
                              name='temp_perfil_ubicaciones',
                              value=data_to_plot, temporary=TRUE,
                              overwrite=T, row.names=FALSE)
      query <- c("INSERT INTO temp_perfil_ubicaciones_t2
                 SELECT * from temp_perfil_ubicaciones;")
      DBI::dbGetQuery(con, query)
      # Clean and disconnect
      gc()
      dbDisconnect(con)
   }
  }
  # Group by into clave (one row per mun/ent)
  tbl(con, dbplyr::sql(glue::glue("SELECT * FROM temp_perfil_ubicaciones_t2"))) %>%
      dplyr::group_by(nivel, nivel_clave) %>%
      dplyr::summarise(values = str_flatten(values, collapse=', ')) %>%
      dplyr::mutate(values = paste0("{",values,'}')) %>%
      compute(name= in_schema("tidy", "perfil_ubicaciones"),temporary=F)

  query <- c("DROP TABLE public.perfil_ubicaciones_t2;")
  DBI::dbGetQuery(con, query)
  print('Closed connection')
}

