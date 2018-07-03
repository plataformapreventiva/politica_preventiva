#!/usr/bin/env Rscript
library(DBI)
library(dbplyr)
library(dplyr)
library(optparse)
library(purrr)
library(rlang)
library(stringr)
library(tidyr)
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
              help="pipeline task", metavar="character")
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
  }

  con <- DBI::dbConnect(RPostgres::Postgres(),
    host = PGHOST,
    port = PGPORT,
    dbname = PGDATABASE,
    user = POSTGRES_USER,
    password = POSTGRES_PASSWORD
  )

  inventario_federales <- tbl(con,  sql('select * from clean.inventario_coneval_federales')) %>%
                         collect()

  inventario_federales_tidy <- inventario_federales %>%
                                      select(entidad_federativa, actualizacion_sedesol, data_date) %>%
                                      tidyr::separate(entidad_federativa, into = paste0('entidad_',1:32), sep = '-', remove = TRUE) %>%
                                      mutate_at(vars(starts_with('entidad')), str_trim) %>%
                                      filter(entidad_1 != 'ND') %>%
                                      gather(header, entidad, -actualizacion_sedesol, -data_date) %>%
                                      filter(!is.na(entidad)) %>%
                                      mutate(entidad = recode(entidad, 'Distrito Federal' = 'Ciudad de México')) %>%
                                      group_by(actualizacion_sedesol, data_date) %>%
                                      count(entidad) %>%
                                      mutate(plot = 's02_estados-federal',
                                             variable = 'coneval_cve_ent',
                                             categoria = str_pad(1:32, 2, pad = '0'),
                                             plot_prefix = 's02_estados') %>%
                                      rename(valor = n) %>%
                                      select(-entidad)


  copy_to(con, inventario_federales_tidy,
          dbplyr::in_schema('tidy', 'inventario_coneval_federales'),
          temporary = FALSE, overwrite = TRUE)


  # disconnect from the database
  dbDisconnect(con)
}


