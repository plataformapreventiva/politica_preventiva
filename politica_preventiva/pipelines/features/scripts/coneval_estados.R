#!/usr/bin/env Rscript
library(optparse)
library(dbplyr)
library(tidyverse)
library(DBI)
library(yaml)
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
              help="database host name", metavar="character")
)

opt_parser <- OptionParser(option_list=option_list)

opt <- tryCatch(
  {
    parse_args(opt_parser)
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

  if(opt$data_date == ""){
    stop("Did not receive a valid data date, stopping", call.=FALSE)
  }else{
    data_date <- opt$data_date
  }
  
  con <- DBI::dbConnect(RPostgres::Postgres(),
                        host = PGHOST,
                        port = PGPORT,
                        dbname = PGDATABASE,
                        user = POSTGRES_USER,
                        password = POSTGRES_PASSWORD
  )
  
  coneval_ent <- tbl(con, sql("select cve_ent, data_date, pobreza, pobreza_e, pobreza_m, factor,
                            vul_car, vul_ing, no_pobv, ic_rezedu, ic_asalud, ic_segsoc,
                            ic_cv, ic_sbv, ic_ali, carencias, carencias3, plb, plb_m, actualizacion_sedesol
                            from clean.coneval_estados"))

  key <- "variable"
  value <- "valor"
  not_gathered <- c("cve_ent", "data_date", "factor", "actualizacion_sedesol")

  coneval_larga <- gather_db(coneval_ent, key, value, not_gathered)

  base <- coneval_larga %>%
    group_by(cve_ent, data_date, variable, actualizacion_sedesol) %>%
    summarise(nominal = sum(factor*valor))

  pob <- coneval_ent %>%
    select(cve_ent,data_date,factor) %>%
    group_by(cve_ent,data_date) %>%
    summarise(pob_tot = sum(factor))

  base <- base %>%
    left_join(pob, by = c("cve_ent", "data_date")) %>%
    mutate(pob_tot = 1.0*pob_tot) %>%
    mutate(porcentaje = nominal/pob_tot)

  act <- base %>% compute(name="temp_coneval_estado_features")

  qu <- "select * from temp_coneval_estado_features"
  ej <- dbGetQuery(con, qu)

  aux_p <- ej %>% select(cve_ent, data_date, variable, porcentaje)
  aux_p_ancha <- spread(aux_p, variable, porcentaje)
  colnames(aux_p_ancha) <- c("cve_ent","data_date",paste0(colnames(aux_p_ancha)[-c(1:2)],"_porcentaje"))

  aux_n <- ej %>% select(cve_ent, data_date, variable, nominal)
  aux_n_ancha <- spread(aux_n, variable, nominal)
  colnames(aux_n_ancha) <- c("cve_ent","data_date",paste0(colnames(aux_n_ancha)[-c(1:2)],"_num"))

  auxiliar <- aux_p_ancha %>%
    left_join(aux_n_ancha, by = c("cve_ent","data_date")) %>%
    dplyr::mutate(actualizacion_sedesol = lubridate::today())

  #dbGetQuery(con, "create table tidy.coneval_estados as (select * from temp_coneval_estado)")

  #dbGetQuery(con, "create index on tidy.coneval_estados(cve_ent)")

  # commit the change
  #dbCommit(con)

  copy_to(con, auxiliar,
          dbplyr::in_schema("features",'coneval_estados'),
          temporary = FALSE, overwrite = TRUE)

  # disconnect from the database
  dbDisconnect(con)
  
  print('Features written to: features.coneval_estados')
}