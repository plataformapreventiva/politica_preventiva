#!/usr/bin/env Rscript
library(optparse)
library(dbplyr)
library(dplyr)
library(DBI)
library(yaml)
library(stringr)

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
  
  cfe <- tbl(con, dbplyr::in_schema('clean','cfe'))
  hospitales <- tbl(con, dbplyr::in_schema('features','recursos_hospitales'))
  cngmd_drenaje <- tbl(con, dbplyr::in_schema('clean','cngmd_drenaje')) 
  cngmd_recoleccion_basura <- tbl(con, dbplyr::in_schema('clean','cngmd_recoleccion_basura'))
  # inernet <- tbl(con, dbplyr::in_schema('clean','internet'))
  gestion_riesgo <- tbl(con, dbplyr::in_schema('clean','cngmd_proteccion_civil'))
  
  capacidad_infraestructura_mun <- left_join(cfe,cngmd_drenaje, by= 'cve_muni') %>%
    left_join(hospitales, by='cve_muni') %>%
    left_join(cngmd_recoleccion_basura,by='cve_muni') %>% retrieve_result()
    dplyr::mutate(actualizacion_sedesol = lubridate::today())
  
  copy_to(con, capacidades_municipios,
          dbplyr::in_schema("features",'capacidad_infraestructura_mun'),
          temporary = FALSE, overwrite = TRUE)
  dbDisconnect(con)
  
  print('Features written to: features.capacidad_infraestructura_mun')
}