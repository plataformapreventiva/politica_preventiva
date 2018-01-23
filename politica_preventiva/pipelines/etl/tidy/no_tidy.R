#!/usr/bin/env Rscript
library(optparse)
library(tidyverse)
library(dbplyr)
library(DBI)
library(RPostgreSQL)

option_list = list(
  make_option(c("--datadate"), type="character", default="", 
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


PGDATABASE <- opt$database
POSTGRES_PASSWORD <- opt$password
POSTGRES_USER <- opt$user
PGHOST <- opt$host
PGPORT <- "5432"
pipeline_task <- opt$pipeline

con <- dbConnect(RPostgres::Postgres(),
    host = PGHOST,
    port = PGPORT,
    dbname = PGDATABASE,
    user = POSTGRES_USER,
    password = POSTGRES_PASSWORD
)


query = sprintf("drop table if exists tidy.%s; create table tidy.%s as (select * from clean.%s;",
	pipeline_task,
	pipeline_task,
	pipeline_task)

dbGetQuery(con, query)

# commit the change
dbCommit(con)

# disconnect from the database
dbDisconnect(con)

