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

  query <- glue::glue("DROP TABLE IF EXISTS temp_{pipeline_task}_t2;")
  DBI::dbGetQuery(con, query)
  query_2 <- glue::glue("CREATE TABLE IF NOT EXISTS  temp_{pipeline_task}_t2 (
               nivel TEXT,
               nivel_clave TEXT,
               variable TEXT,
               values TEXT);")
  DBI::dbGetQuery(con, query_2)

  dbDisconnect(con)
  for(level in as.list(strsplit(opt$extra_parameters, ",")[[1]])){

    con <- DBI::dbConnect(RPostgres::Postgres(),
                          host=PGHOST,
                          port=PGPORT,
                          dbname=PGDATABASE,
                          user=POSTGRES_USER,
                          password=POSTGRES_PASSWORD
    )
    if (level=='m'){
      key_var <- 'cve_muni'
      n <- 5
    } else{
      key_var <- 'cve_ent'
      n <-2
    }
    print(key_var)

    time_series <- c('amenazas_violencia', 'poblacion_pobreza', 'pobreza_mult', 'carencias')
    plots_data <- tbl(con, dbplyr::sql(glue::glue("SELECT nivel, plot, plot_type, metadata \
                                                 FROM plots.{pipeline_task} \
                                                  WHERE nivel = '{level}'"))) %>%
      dplyr::collect() %>%
      dplyr::filter(!grepl('^programas_', plot),
                    !grepl('^distribucion_', plot),
                    !grepl('^poblacion_atendida', plot),
                    !grepl('^capacidad_institucional', plot)) %>%
      # Los mapas, pub y las series de tiempo se hacen en otro pipeline.
      dplyr::filter(plot_type != "map" & !(plot %in% time_series))  %>%
      dplyr::distinct(plot, .keep_all = TRUE)

    plots_metadata <- textConnection(dplyr::pull(plots_data, metadata) %>%
                                       gsub("\\n", "", .)) %>%
      jsonlite::stream_in() %>%
      dplyr::bind_cols(plots_data, .) %>%
      unnest(vars=vars)

    plots_metadata_d <- plots_metadata %>%
      rowwise %>%
      mutate(metadata = toJSON(list(plot_type=plot_type,
                                    vars = vars,
                                    metadata=metadata,
                                    title=title,
                                    palette=palette,
                                    subtext=subtext),auto_unbox=T)) %>%
      dplyr::select(plot, metadata)

    sql_queries <- purrr::map_chr(1:nrow(plots_metadata),
                                  function(x) glue::glue("SELECT {paste(c(key_var,
                                                         plots_metadata$vars[[x]]),
                                                         collapse = ', ')}
                             FROM {plots_metadata$schema[x]}.{plots_metadata$table_name[x]}"))


    tidy_data <- tibble::tibble()
    concat <- tibble::tibble()

    for(i in 1:nrow(plots_metadata)){
        con <- DBI::dbConnect(RPostgres::Postgres(),
          host=PGHOST,
          port=PGPORT,
          dbname=PGDATABASE,
          user=POSTGRES_USER,
          password=POSTGRES_PASSWORD
        )

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
            dplyr::mutate(nivel_clave = str_pad(nivel_clave,n,"left", '0'),
                          nivel = level) %>%
            dplyr::arrange(variable) %>%
            select(nivel, nivel_clave, variable, valor)

        RPostgres::dbWriteTable(conn=con,
                                name=glue::glue('temp_{pipeline_task}'),
                                value=data_to_plot, temporary=TRUE,
                                overwrite=T, row.names=FALSE)

        query <- glue::glue("INSERT INTO temp_{pipeline_task}_t2
                   SELECT * from temp_{pipeline_task}")
        DBI::dbGetQuery(con, query)
        # Clean and disconnect
        gc()
        dbDisconnect(con)
   }
  }
  print('Closed connection')
  dbDisconnect(con)
}
