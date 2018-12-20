#!/usr/bin/env Rscript
library(optparse)
library(dbplyr)
library(dplyr)
library(DBI)
library(lubridate)
library(yaml)
library(aws.s3)

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

  source("pipelines/features/tools/features_tools.R")

  print('Pulling datasets')

  query <- "SELECT d_llave, cve_ent, cve_muni, cve_muni || cve_loc as cve_locc, cve_scian, raz_soc,
    desc_scian, coord_y, coord_x, nom_est FROM clean.denue
    WHERE cve_scian ~ '(^61|^62)'"
  data <- tbl(con, sql(query)) %>%
      rename(lat=coord_y, long=coord_x) %>%
      mutate(type = ifelse( grepl('^61',cve_scian),
                           "Servicios Educativos",
                           "Servicios de Salud"),
             element_id = d_llave,
             nivel="georreferencia",
             clave = cve_muni)  %>%
      select(nivel, clave, element_id, type,
             raz_soc, cve_scian, desc_scian,lat, long)
  copy_to(con, data,
          dbplyr::in_schema("features","denue_servicios"),
          temporary  = FALSE, overwrite = TRUE)
  dbDisconnect(con)

  print('Features written to: features.denue_servicios')
}
