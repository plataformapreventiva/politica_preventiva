#!/usr/bin/env Rscript
library(optparse)
library(dbplyr)
library(dplyr)
library(DBI)
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

query <- 'SELECT LPAD(claveentidad::text, 2, \'0\') || LPAD(clavemunicipio::text, 3, \'0\') as cve_muni, 
  avg(CAST(e1819+e20 as float)/1000) as densidad_medicos, 
  avg(CAST(unidades+e13 as float)/1000) as densidad_hospitales, 
  avg(CAST(e14+e15 as float)/1000) as densidad_camas
  FROM raw.recursos_hospitales
  GROUP BY cve_muni'

recursos_hospitales <- tbl(con, sql(query)) %>%
  dplyr::mutate(data_date = data_date,
                actualizacion_sedesol = lubridate::today()) %>%
  dplyr::select(cve_muni,densidad_medicos,densidad_hospitales,
                densidad_camas,data_date,actualizacion_sedesol)

copy_to(con, recursos_hospitales,
          dbplyr::in_schema("features",'recursos_hospitales_municipios'),
          temporary = FALSE, overwrite = TRUE)

dbDisconnect(con)
  
print('Features written to: recursos_hospitales_municipios')
}