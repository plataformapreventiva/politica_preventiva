#!/usr/bin/env Rscript
library(optparse)
library(tidyverse)
library(dbplyr)
library(DBI)
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
              help="database host name", metavar="character"),
  make_option(c("--pipeline"), type="character", default="",
	      help="pipeline taks", metavar="character")
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

  con <- dbConnect(RPostgres::Postgres(),
    host = PGHOST,
    port = PGPORT,
    dbname = PGDATABASE,
    user = POSTGRES_USER,
    password = POSTGRES_PASSWORD
  )

  print(con)
  dbListTables(con)

  # Read table from clean
  coneval_municipios <- tbl(con, sql("select * from clean.coneval_municipios"))

  key <- "variable"
  value <- "valor"
  not_gathered <- c("ent", "entidad",
                    "cve_muni", "municipio",
                    "pob_tot",
                    "actualizacion_sedesol", "data_date")
  selects_cols <- c('ent', 'cve_muni', 'pob_tot','variable', 'valor', 'pob_tot',
                    'data_date', 'actualizacion_sedesol')

  # Gather table to long form
  coneval_larga <- gather_db(coneval_municipios, key, value, not_gathered) %>%
                   select(selects_cols) %>%
                   compute(name='temp_coneval_municipios')

  # query for cleaning variable and adding tipo
  query_clean = c("CREATE TABLE tidy.coneval_municipios AS (
                        SELECT ent,
                          cve_muni,
                          pob_tot,
                          CASE WHEN (variable ~ '_porcentaje')
                             THEN regexp_replace(variable, '_porcentaje', '')
                             ELSE CASE WHEN (variable ~ '_num')
                                THEN regexp_replace(variable, '_num', '')
                                ELSE variable
                                END
                             END AS variable,
                          CASE WHEN (variable ~ '_porcentaje') OR (variable ~ 'int_')
                          THEN 'porcentaje'
                          ELSE CASE WHEN (variable ~ '_num') OR (variable ~ '_carencias')
                               THEN 'nominal'
                               ELSE ''
                               END
                          END AS tipo,
                          valor,
                          data_date,
                          actualizacion_sedesol
                          FROM temp_coneval_municipios)")

  dbGetQuery(con, sql(query_clean))
  dbCommit(con)
  dbDisconnect(con)
}
