#!/usr/bin/env Rscript
library(optparse)
library(RPostgreSQL)
library(tidyverse)
library(dbplyr)
library(dplyr)

option_list = list(
  make_option(c("-d", "--datadate"), type="character", default="2016-a", 
              help="data date", metavar="character"),
  make_option(c("-d", "--database"), type="character", default="", 
              help="database name", metavar="character"),
  make_option(c("-u", "--user"), type="character", default="",
              help="database user", metavar="character"),
  make_option(c("-p", "--password"), type="character", default="2016-a",
              help="password for datbase user", metavar="character"),
  make_option(c("-h", "--host"), type="character"),
              help="database host name", metavar="character")
);

opt_parser <- OptionParser(option_list=option_list);
opt <- parse_args(opt_parser);

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

gather_db <- function(df, key, value, not_gathered) {
  key_vals <- setdiff(colnames(df),not_gathered)
  
  get_rows <- function(key_val) {
    df %>%
      select_(.dots = c(not_gathered, key_val)) %>%
      mutate(key_ = as.character(key_val)) %>%
      rename_(value_ = key_val) %>%
      select_(.dots = c(not_gathered, "key_", "value_"))
  }
  
  df_new <- lapply(key_vals, get_rows)
  Reduce(union_all, df_new) %>%
    rename_(.dots= setNames("key_", key)) %>%
    rename_(.dots= setNames("value_", value)) 
}

spread_db <- function(df, key, value) {
  key_vals <- 
    df %>%
    select_(key) %>%
    distinct() %>%
    collect() %>%
    .[[1]] %>%
    sort()
  
  get_rows <- function(key_val) {
    df %>%
      rename_(.dots= setNames(key, "key")) %>%
      rename_(.dots= setNames(value, key_val)) %>%
      filter(key == key_val) %>%
      select(-key)
  }
  
  join_vars <- setdiff(colnames(df), c(key, value))
  print(join_vars)
  full_join_alt <- function(x, y) {
    full_join(x, y, by=join_vars)
  }
  
  df_new <- lapply(key_vals, get_rows)
  Reduce(full_join_alt, df_new) 
}

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
