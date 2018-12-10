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
  
  query1 <- 'SELECT LEFT(cve_muni, 2) as cve_ent,
               acceso_a_la_justicia,
              reglamentacion_basica,
              gobernabilidad,
              finanzas_publicas,
              capacidad_economica,
              administrativa__operativa
               FROM models.inform_index_municipios'
  
  media_mun <- tbl(con, sql(query1)) %>% group_by(cve_ent)  %>%
    summarise_all(mean,na.rm = TRUE) %>% collect()
  
  media <- tbl(con, sql(query1)) %>% select(-cve_ent) %>%
    summarise_all(sum,na.rm = TRUE) %>% collect()
  
  media_nacional <-  data.frame(cve_ent='00',media)
  
  capacidad_comparaciones <- rbind(media_mun,media_nacional)

  copy_to(con, capacidad_comparaciones,
          dbplyr::in_schema("features",'capacidad_comparaciones'),
          temporary = FALSE, overwrite = TRUE)
  dbDisconnect(con)
  
  print('Features written to: features.capacidad_comparaciones')
}
