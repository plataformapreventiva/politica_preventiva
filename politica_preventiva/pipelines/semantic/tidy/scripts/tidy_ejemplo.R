#!/usr/bin/env Rscript
library(optparse)
library(tidyverse)
library(dbplyr)
library(stringr)
library(DBI)
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
              help="pipeline task", metavar="character")
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

  df <- tbl(con, dplyr::in_schema('clean', 'delitos_comun'))

  key <- "variable"
  value <- "valor"

  if(level == 'e'){
      key_var <- 'cve_ent'
  } elif(level == 'm') {
      key_var <- 'cve_muni'
  } else {
      key_var <- NULL
  }

  not_gathered <- c(key_var, "data_date", "actualizacion_sedesol")

  df_largo <- gather_db(df, key, value, not_gathered) %>%
                dplyr::compute(name="df_temp")

  df_variables <- tbl(con, sql(glue::glue("select {key_var} as nivel_clave, variable from df_temp"))) %>%
                    gather_db(varname, value, "nivel_clave") %>%
                    dplyr::mutate(nivel = level,
                                  plot = plot_task,
                                  element_id = rownames(.),
                                  vartype = 'x') %>%
                    dplyr::select(nivel, nivel_clave, plot, element_id,
                                  vartype, varname, value, label) %>%
                    dplyr::compute()

  df_valores <- tbl(con, sql(glue::glue("select {key_var} as nivel_clave, valor from df_temp"))) %>%
                    gather_db(varname, value, "nivel_clave") %>%
                    dplyr::mutate(nivel = level,
                                  plot = plot_task,
                                  element_id = rownames(.),
                                  vartype = 'y') %>%
                    dplyr::select(nivel, nivel_clave, plot, element_id,
                                  vartype, varname, value, label) %>%
                    dplyr::compute()

  dbGetQuery(con, glue::glue("CREATE TABLE tidy.{plot_task} AS (SELECT * FROM df_temp_variables) UNION (SELECT * FROM df_temp_valores)))"))

  dbCommit(con)

  dbDisconnect(con)

}
