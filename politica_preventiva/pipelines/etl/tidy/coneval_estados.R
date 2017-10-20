#!/usr/bin/env Rscript
library(optparse)
library(RPostgreSQL)
library(tidyverse)
library(dbplyr)
library(dplyr)
source("pipelines/etl/tools/tidy_tools.R") 

option_list = list(
  make_option(c("--datadate"), type="character", default="2016-a", 
              help="data date", metavar="character"),
  make_option(c("--database"), type="character", default="", 
              help="database name", metavar="character"),
  make_option(c("--user"), type="character", default="",
              help="database user", metavar="character"),
  make_option(c("--password"), type="character", default="2016-a",
              help="password for datbase user", metavar="character"),
  make_option(c("--host"), type="character", default="",
              help="database host name", metavar="character")
);

opt_parser <- OptionParser(option_list=option_list);

opt <- tryCatch(
        {
          parse_args(opt_parser);
        },
        error=function(cond) {
            message("Error:")
            message(cond)
            return(NA)
        },
        warning=function(cond) {
            message("Warning:")
            message(cond)
            return(NULL)
        },
        finally={
            message("Arguments succesfully parsed.")
        }
    )  

if(!is.na(opt) & !is.null(opt)){

  if (is.null(opt$database) | is.null(opt$user) | 
      is.null(opt$password) | is.null(opt$host) ){
    print_help(opt_parser)
    stop("Database connection arguments are not supplied.n", call.=FALSE)
  }else{
    PGDATABASE <- opt$database
    POSTGRES_PASSWORD <- opt$password
    POSTGRES_USER <- opt$user
    PGHOST <- opt$host
    PGPORT <- 5432
  }

  drv <- dbDriver("PostgreSQL")
  con <- dbConnect(drv, dbname = PGDATABASE,
                   host = PGHOST, port = PGPORT,
                   user = POSTGRES_USER, password = POSTGRES_PASSWORD)

  dbListTables(con)
  coneval_ent <- tbl(con, sql("select cve_ent, data_date, pobreza, pobreza_e, pobreza_m, factor,
                              vul_car, vul_ing, no_pobv, ic_rezedu, ic_asalud, ic_segsoc, 
                              ic_cv, ic_sbv, ic_ali, carencias, carencias3, plb, plb_m 
                              from raw.coneval_estados"))

  key <- "variable"
  value <- "valor"
  not_gathered <- c("cve_ent", "data_date", "factor")

  coneval_larga <- gather_db(coneval_ent, key, value, not_gathered) 

  base <- coneval_larga %>% 
    group_by(cve_ent, data_date, variable) %>%
    summarise(num = sum(factor*valor))

  pob <- coneval_larga %>%
    select(cve_ent,data_date,factor) %>%
    group_by(cve_ent,data_date) %>%
    summarise(pob_tot = sum(factor))

  base <- base %>%
    left_join(pob, by = c("cve_ent", "data_date")) %>%
    mutate(pob_tot = 1.0*pob_tot) %>%
    mutate(porcentaje = num/pob_tot) %>%
    compute() 

  base <- base %>% collect()

  dbWriteTable(conn = con, c("tidy", "coneval_estados"), value = base,
               overwrite=TRUE, row.names=FALSE)

  dbGetQuery(con, "create index on tidy.coneval_estados(cve_ent)")

  # commit the change
  dbCommit(con)

  # disconnect from the database
  dbDisconnect(con)
}

