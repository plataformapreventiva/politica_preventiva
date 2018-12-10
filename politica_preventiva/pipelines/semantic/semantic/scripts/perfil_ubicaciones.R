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

  dictionary_rows <- tibble()
  for(i in 1:nrow(plots_metadata)){
      dict_schema <- dplyr::recode(plots_metadata$schema[i], clean='raw')
      table_name <- plots_metadata$table_name[i]
      varlist <- plots_metadata$var_list[i]
      var_labels <- tbl(con, dbplyr::sql(glue::glue("SELECT id, nombre \
                                                    FROM {dict_schema}.{table_name}_dic \
                                                    WHERE id = ANY({varlist})")))
      dictionary_rows <- dplyr::bind_rows(var_labels, dictionary_rows)
  }

  tidy_data <- tbl(con, dbplyr::sql(glue::glue("SELECT * \
                                                FROM tidy.{pipeline_task} \
                                                WHERE nivel = '{level}'")))

  # Jalar varname de x, buscar su label y meterlo al diccionario

  dplyr::copy_to(con, semantic_data,
                 dbplyr::in_schema('semantic', pipeline_task),
                 temporary = FALSE)
}
