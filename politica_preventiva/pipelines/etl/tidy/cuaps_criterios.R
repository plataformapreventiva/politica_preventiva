#!/usr/bin/env Rscript
library(optparse)
library(tidyverse)
library(dbplyr)
library(stringr)
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

  # Auxiliary subsetting helpers

  criterios_grupos_vulnerables <- list('36' = 'r_1',
                                    '1754' = 'r_1',
                                    '1755' = 'r_1',
                                    '1756' = 'r_1',
                                    '1757' = 'r_1',
                                    '37' = 'r_6',
                                    '38' = 'r_2',
                                    '39' = 'r_4',
                                    '40' = 'r_5',
                                    '41' = 'r_3',
                                    '42' = 'r_8',
                                    '167' = 'r_8',
                                    '168' = 'r_8',
                                    '169' = 'r_8',
                                    '43' = 'r_7',
                                    '170' = 'r_7',
                                    '171' = 'r_7',
                                    '172' = 'r_7',
                                    '173' = 'r_7',
                                    '174' = 'r_7',
                                    '175' = 'r_7',
                                    '176' = 'r_7',
                                    '44' = 'r_9')
  criterios_pobreza <- 31:35
  criterios_carencias <- 1:6
  criterios_territorio <- 130:133
  criterios_zap <- 158:160
  criterios_marginacion <- 134:145

  # Fetch data
  cuaps_criterios <- tbl(con, sql("select * from clean.cuaps_criterios")) %>%
      collect()

  cuaps_criterios <- cuaps_criterios %>%
                        mutate(atiende_grupos_vulnerables = recode(filter_values(csc_configuracion_foc,
                                                                                 names(criterios_grupos_vulnerables)),
                                                                   !!! criterios_grupos_vulnerables),
                               atiende_pobreza = filter_values(csc_configuracion_foc,
                                                               criterios_pobreza),
                               atiende_carencias = filter_values(csc_configuracion_foc,
                                                                 criterios_carencias),
                               atiende_territorio = filter_values(csc_configuracion_foc,
                                                                  criterios_territorio),
                               atiende_zap = filter_values(csc_configuracion_foc,
                                                           criterios_zap),
                               atiende_marginacion = filter_values(csc_configuracion_foc,
                                                                   criterios_marginacion))


  varnames <- c('atiende_grupos_vulnerables', 'atiende_pobreza',
                          'atiende_carencias', 'atiende_territorios', 'atiende_zap',
                          'atiende_marginacion')

  plotnames <- c('s04_grupos_vulnerables', 's05_pobreza', 's07_carencias',
                           's08_territorios', 's08_zap', 's08_marginacion')

  subsets <- list('atiende_grupos_vulnerables' = criterios_grupos_vulnerables,
                  'atiende_pobreza' = criterios_pobreza,
                  'atiende_carencias' = criterios_carencias,
                  'atiende_territorios' = criterios_territorios,
                  'atiende_zap' = criterios_zap,
                  'atiende_marginacion' = criterios_marginacion)

  names_df <- create_varnames_data(cuaps_criterios, varnames, plotnames, subsets)

  cuaps_criterios_tidy <- map_df(1:nrow(names_df), function(x) tidy_count(data = cuaps_criterios,
                                                                          count_var = names_df$varname[x],
                                                                          plotname = names_df$plotname[x],
                                                                          subset = names_df$subset[[x]]))

  copy_to(con, cuaps_criterios_tidy,
          dbplyr::in_schema('tidy', 'cuaps_criterios'),
          temporary = FALSE, overwrite = TRUE)


  # disconnect from the database
  dbDisconnect(con)
}
