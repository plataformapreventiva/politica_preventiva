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

  poblacion <- tbl(con, dbplyr::in_schema('public',
                                          'poblacion_intercensal_mun_2015'))

  selected_crimes <- c('Homicidio doloso',
                       'Homicidio culposo',
                       'Secuestro',
                       'Feminicidio',
                       'Violencia familiar',
                       'Robo de vehículo automotor',
                       'Daño a la propiedad')

  crimenes_tasas <- tbl(con, dbplyr::in_schema('clean', 'delitos_comun')) %>%
                    dplyr::filter(subtipodedelito %in% selected_crimes) %>%
                    dplyr::group_by(anio, mes, cve_muni, subtipodedelito) %>%
                    dplyr::summarise(incidencia_total =  sum(incidencia_delictiva,
                                                             na.rm = TRUE)) %>%
                    dplyr::left_join(poblacion) %>%
                    dplyr::mutate(tasa = incidencia_total/poblacion) %>%
                    dplyr::select(-poblacion) %>%
                    spread_db(., 'subtipodedelito', 'tasa') %>%
                    dplyr::rename(danio_prop = `Daño a la propiedad`,
                                  feminicidio = `Feminicidio`,
                                  homicidio_culposo = `Homicidio culposo`,
                                  homicidio_doloso = `Homicidio doloso`,
                                  secuestro = `Secuestro`,
                                  violencia_familiar = `Violencia familiar`,
                                  robo_vehiculos = `Robo de vehículo automotor`) %>%
                    dplyr::mutate(data_date = data_date,
                                  actualizacion_sedesol = lubridate::today())

  copy_to(con, crimenes_tasas,
          dbplyr::in_schema("features",'crimenes_tasas'),
          temporary = FALSE, overwrite = TRUE)
  dbDisconnect(con)

  print('Features written to: features.crimenes_tasas')
}
