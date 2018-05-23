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


  cuaps_programas <- tbl(con,  sql('select * from clean.cuaps_programas')) %>%
                         collect()


  varnames <- c('orden_gob', 'cve_entidad_federativa', 'der_social')
  plotnames <- c('s01_ordengob', 's02_estados', 's03_derechos')
  subsets <- list('orden_gob' = 1:3,
                'cve_entidad_federativa' = as.character(1:32),
                'der_social' = 1)

  names_df <- create_varnames_data(cuaps_programas, varnames, plotnames, subsets) %>%
                filter(varname != 'der_social_ning')


  cuaps_programas_tidy <- map_df(1:nrow(names_df), function(x) tidy_count(data = cuaps_programas,
                                                     count_var = names_df$varname[x],
                                                     plotname = names_df$plotname[x],
                                                     subset = names_df$subset[[x]]))

  copy_to(con, cuaps_programas_tidy,
          dbplyr::in_schema('tidy', 'cuaps_programas'),
          temporary = FALSE, overwrite = TRUE)


  # disconnect from the database
  dbDisconnect(con)
}
