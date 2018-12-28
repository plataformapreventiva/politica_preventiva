#!/usr/bin/env Rscript
library(DBI)
library(dbplyr)
library(jsonlite)
library(optparse)
library(tidyverse)
library(stringr)
source("pipelines/semantic/tools/tidy_tools.R")

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
  make_option(c("--prod_database"), type="character", default="",
              help="prod database name", metavar="character"),
  make_option(c("--prod_user"), type="character", default="",
              help="prod database user", metavar="character"),
  make_option(c("--prod_password"), type="character", default="",
              help="prod password for datbase user", metavar="character"),
  make_option(c("--prod_host"), type="character", default="",
              help="prod database host name", metavar="character"),
  make_option(c("--pipeline"), type="character", default="",
              help="semantic pipeline task", metavar="character"),
  make_option(c("--extra_parameters"), type="character", default="",
              help="Extra parameters", metavar="character")
);


opt_parser <- OptionParser(option_list=option_list);

opt <- tryCatch(
        {
          parse_args(opt_parser);
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
    PROD_DATABASE <- opt$prod_database
    PROD_PASSWORD <- opt$prod_password
    PROD_USER <- opt$prod_user
    PROD_HOST <- opt$prod_host
    PROD_PORT <- "5432"


  }

  con <- DBI::dbConnect(RPostgres::Postgres(),
    host = PGHOST,
    port = PGPORT,
    dbname = PGDATABASE,
    user = POSTGRES_USER,
    password = POSTGRES_PASSWORD
  )


  pipeline_task <- opt$pipeline

  query <- glue::glue("DROP TABLE IF EXISTS semantic.{pipeline_task};")
  #DBI::dbGetQuery(prod_con, query)
  semantic_data <- tibble()

  # TODO() take table names from config yaml
  tables <- unlist(strsplit('perfil_ubicaciones,temporal_ubicaciones,maps_ubicaciones', ","))
  queries <- purrr::map_chr(1:length(tables),
                            function(x) glue::glue("SELECT '{tables[x]}' as tipo,
                                                           nivel, nivel_clave,
                                                           values::JSONB
                                                    FROM tidy.{tables[x]}"))

  queries <- paste(queries, collapse=' UNION ')
  data <- tbl(con, dbplyr::sql(queries)) %>% collect()
  db_schema <- c(tipo='TEXT', nivel='TEXT', nivel_clave='TEXT',values='JSONB')
  RPostgres::dbWriteTable(conn=con,
                          name=c("semantic",'perfil'),
                          value=data,
                          temporary=FALSE,
                          overwrite=TRUE,
                          field.types=db_schema,
                          row.names=FALSE)

  prod_con <- DBI::dbConnect(RPostgres::Postgres(),
    host = PROD_HOST,
    port = PROD_PORT,
    dbname = PROD_DATABASE,
    user = PROD_USER,
    password = PROD_PASSWORD
  )

  RPostgres::dbWriteTable(conn=prod_con,
                          name=c("semantic",'perfil'),
                          value=data,
                          temporary=FALSE,
                          overwrite=TRUE,
                          field.types=db_schema,
                          row.names=FALSE)
}
