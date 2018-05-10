#!/usr/bin/env Rscript
library(optparse)
library(tidyverse)
library(dbplyr)
library(stringr)
library(DBI)
source("pipelines/etl/tools/tidy_tools.R")

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

  cuaps_programas <- tbl(con, sql("select * from clean.cuaps_programas"))

  key <- "variable"
  value <- "valor"
  not_gathered <- c("cuaps_folio", "data_date", "actualizacion_sedesol")

  cuaps_programas_long <- gather_db(cuaps_programas, key, value, not_gathered) %>%
    select(clave=cuaps_folio, variable, valor, data_date, actualizacion_sedesol) %>%
    compute(name="cuaps_programas_temp")

  dbGetQuery(con, "create table tidy.cuaps_programas as (select * from cuaps_programas_temp)")

  # commit the change
  dbCommit(con)

  # disconnect from the database
  dbDisconnect(con)
}
