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

  cenapred <- tbl(con, sql("select cve_muni,data_date,gp_bajaste,gp_ciclnes,
                          gp_granizo,gp_inundac,gp_nevadas,gp_sequia2,gp_sismico,
                          gp_susinfl,gp_sustox,gp_tormele,gp_tsunami,gp_ondasca, 
                          actualizacion_sedesol
                          from clean.cenapred"))

  key <- "variable"
  value <- "valor"
  not_gathered <- c("cve_muni", "data_date", "actualizacion_sedesol")

  cenapred_larga <- gather_db(cenapred, key, value, not_gathered) %>%
    select(clave=cve_muni, variable, valor, data_date, actualizacion_sedesol) %>%
    compute(name="cenapred_temp")

  dbGetQuery(con, "create table tidy.cenapred as (select * from cenapred_temp)")

  # commit the change
  dbCommit(con)

  # disconnect from the database
  dbDisconnect(con)
}