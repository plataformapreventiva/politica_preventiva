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

  dependencias_federales = list('06' = 'SHCP',
                                '08' = 'SEDATU',
                                '10' = 'SE',
                                '11' = 'SEP',
                                '12' = 'SS',
                                '14' = 'STPS',
                                '15' = 'SEDATU',
                                '16' = 'SEMARNAT',
                                '20' = 'SEDESOL',
                                '48' = 'SC',
                                '50' = 'IMSS',
                                '1900' = 'IMSS-PROSPERA',
                                'c-bansefi' = 'BANSEFI',
                                'c-agroasemex' = 'AGROASEMEX',
                                'c-cdi' = 'CDI',
                                'c-conacyt' = 'CONACYT',
                                'c-inmujeres' = 'INMUJERES',
                                'c-issste' = 'ISSSTE',
                                'c-sectur' = 'SECTUR')

  listado_coneval_federales <- tbl(con,  sql('select * from clean.listado_coneval_federales')) %>%
                         collect()

  varnames <- c('dependencia')
  plotnames <- c('s03_dependencias-federal')
  subsets <- list('dependencia' = unlist(unname(dependencias_federales)))

  names_df <- create_varnames_data(listado_coneval_federales, varnames, plotnames, subsets)

  listado_coneval_federales_tidy <- map_df(1:nrow(names_df), function(x) tidy_count(data = listado_coneval_federales,
                                                     count_var = names_df$varname[x],
                                                     plotname = names_df$plotname[x],
                                                     subset = names_df$subset[[x]])) %>%
                                    replace_na(list(actualizacion_sedesol = pull_filler(listado_coneval_federales, 'actualizacion_sedesol'),
                                            data_date = pull_filler(listado_coneval_federales, 'data_date'))) %>%
                                    mutate(plot_prefix = gsub('(s.*)(-.*)', '\\1', plot))

  copy_to(con, listado_coneval_federales_tidy,
          dbplyr::in_schema("tidy", "listado_coneval_federales"),
          temporary = FALSE, overwrite = TRUE)

  # disconnect from the database
  dbDisconnect(con)
}



