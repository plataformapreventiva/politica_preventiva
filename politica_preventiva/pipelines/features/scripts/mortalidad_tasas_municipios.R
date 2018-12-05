#!/usr/bin/env Rscript
library(optparse)
library(dbplyr)
library(dplyr)
library(DBI)
library(lubridate)
library(yaml)

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
  
  poblacion <- tbl(con, dbplyr::in_schema('clean','intercensal_personas_2015')) %>% 
    select(cve_muni, factor) %>%
    group_by(cve_muni) %>% 
    summarise(factor=as.numeric(sum(factor)))
  
query <- 'SELECT cve_muni, sum(CASE 
                                            WHEN CAST(edad as integer) < 4000
                                            THEN 1 
                                            ELSE 0 
                                            END) as infant, 
                                            sum(CASE 
                                            WHEN causa_def LIKE \'O%\'
                                            THEN 1 
                                            ELSE 0 
                                            END) as materna
                                            FROM clean.defunciones_generales
                                            GROUP BY cve_muni'
mortalidad_tasas <- tbl(con, sql(query)) %>%
    dplyr::left_join(poblacion, by='cve_muni') %>%
    dplyr::mutate(mortalidad_materna = materna/factor) %>%
    dplyr::mutate(mortalidad_infantil = infant/factor) %>%
    dplyr::select(cve_muni,mortalidad_materna,mortalidad_infantil) %>%
    dplyr::mutate(actualizacion_sedesol = lubridate::today())
  
  copy_to(con, mortalidad_tasas,
          dbplyr::in_schema("features",'mortalidad_tasas_municipios'),
          temporary = FALSE, overwrite = TRUE)
  dbDisconnect(con)
  
  print('Features written to: features.mortalidad_tasas_municipios')
}
