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

  # Athena Connection
  con_a <- pub_connect(s3dir = Sys.getenv("S3_DIR"), schema = Sys.getenv("SCHEMA"))

  # NOTE: Do not erase trailing spaces
  query <- glue::glue("SELECT 'n' nivel, 'nacional' cvenivel, anio as nomperiodo, 
                            'origen-cddependencia-cdpadron' cveagregado, 
                            origen || '-' || cddependencia || '-' || cdprograma as nomagregado, 
                            count(distinct newid) as beneficiarios, 
                            sum(nuimpmonetario) as monto 
                          FROM athena_pub.pub_cleaned_test 
                            GROUP BY anio, origen, cddependencia, cdprograma, cdpadron 
                        UNION 
                          SELECT 'e' nivel, cveent as cvenivel, anio as nomperiodo, 
                            'origen-cddependencia-cdprograma' cveagregado, 
                            origen || '-' || cddependencia || '-' || cdprograma as nomagregado, 
                            count(distinct newid) as beneficiarios, 
                            sum(nuimpmonetario) as monto 
                          FROM athena_pub.pub_cleaned_test 
                            GROUP BY cveent, anio, origen, cddependencia, cdprograma 
                        UNION 
                          SELECT 'm' nivel, cvemuni as cvenivel, anio as nomperiodo, 
                            'origen-cddependencia-cdprograma' cveagregado, 
                            origen || '-' || cddependencia || '-' || cdprograma as nomagregado, 
                            count(distinct newid) as beneficiarios, 
                            sum(nuimpmonetario) as monto 
                          FROM athena_pub.pub_cleaned_test 
                            GROUP BY cvemuni, anio, origen, cddependencia, cdprograma")

  los_queries <- query_dic()
  c(beneficiarios_sedesol_ALL,los_queries) := load_or_run(con_a,query,los_queries)
  beneficiarios_sedesol_ALL[is.na(beneficiarios_sedesol_ALL)] <- 0

  # Get table at municipality level
  copy_to(con, beneficiarios_sedesol_ALL,
          dbplyr::in_schema("features","pub_agregados"),
          temporary = FALSE, overwrite = TRUE)

  dbDisconnect(con)

  print('Features written to: features.pub_agregados')
}
