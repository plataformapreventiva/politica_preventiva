#!/usr/bin/env Rscript
library(optparse)
library(dbplyr)
library(dplyr)
library(DBI)
library(lubridate)
library(yaml)
library(stringr)

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

  poblacion <- tbl(con, dbplyr::in_schema('clean',
                                          'coneval_municipios')) %>%
        dplyr::filter(data_date=='2015-a') %>%
        dplyr::rename(cve_ent=ent) %>%
        dplyr::group_by(cve_ent) %>%
        dplyr::summarise(pob_tot=sum(pob_tot, na.rm=TRUE)) %>%
        dplyr::rename(poblacion = pob_tot) %>%
        dplyr::select(cve_ent, poblacion)


  selected_crimes <- c('Homicidio doloso',
                       'Homicidio culposo',
                       'Secuestro',
                       'Feminicidio',
                       'Violencia familiar',
                       'Robo de vehículo automotor',
                       'Daño a la propiedad')

  crimenes_data <- tbl(con, dbplyr::in_schema('clean', 'delitos_comun')) %>%
                    dplyr::filter(subtipodedelito %in% selected_crimes) %>%
                    dplyr::group_by(anio, mes, cve_ent, subtipodedelito,data_date) %>%
                    dplyr::summarise(incidencia_total =  sum(incidencia_delictiva,
                                                             na.rm = TRUE)) %>%
                    dplyr::left_join(poblacion) %>%
                    dplyr::mutate(tasa = incidencia_total*100000/poblacion) %>%
                    dplyr::select(-poblacion) %>%
                    dplyr::collect()

  incidencia_total <- crimenes_data %>%
                    dplyr::select(-tasa) %>%
                    tidyr::spread(subtipodedelito, incidencia_total) %>%
                    dplyr::rename(danio_prop_total = `Daño a la propiedad`,
                                  feminicidio_total = `Feminicidio`,
                                  homicidio_culposo_total = `Homicidio culposo`,
                                  homicidio_doloso_total = `Homicidio doloso`,
                                  secuestro_total = `Secuestro`,
                                  violencia_familiar_total = `Violencia familiar`,
                                  robo_vehiculos_total = `Robo de vehículo automotor`) %>%
                    dplyr::ungroup()

  tasa_crimenes <- crimenes_data %>%
                    dplyr::select(-incidencia_total) %>%
                    tidyr::spread(subtipodedelito, tasa) %>%
                    dplyr::rename(danio_prop_tasa = `Daño a la propiedad`,
                                  feminicidio_tasa = `Feminicidio`,
                                  homicidio_culposo_tasa = `Homicidio culposo`,
                                  homicidio_doloso_tasa = `Homicidio doloso`,
                                  secuestro_tasa = `Secuestro`,
                                  violencia_familiar_tasa = `Violencia familiar`,
                                  robo_vehiculos_tasa = `Robo de vehículo automotor`) %>%
                    dplyr::ungroup()


  crimenes_tasas <- dplyr::left_join(tasa_crimenes, incidencia_total) %>%
                    dplyr::mutate(data_date = data_date,
                                  actualizacion_sedesol = lubridate::today(),
                                  month = recode(str_sub(mes,1,3), Ene = 'Jan', Apr = 'Abr',
                                                 Ago = 'Aug' , Dic = 'Dec'),
                                  fecha = lubridate::parse_date_time(paste0('01 ', tolower(month), anio),
                                                                     orders = 'd b Y'),
                                  data_date_text = paste0('01 ', gsub('([0-9]{4})-([0-9]+)(.*)', '\\1 \\2', data_date)),
                                  data_date_parsed = lubridate::parse_date_time(data_date_text,
                                                                             orders = 'd Y m')) %>%
                    dplyr::filter(fecha <= data_date_parsed) %>%
                    dplyr::select(-data_date_text, -data_date_parsed, -month)

  copy_to(con, crimenes_tasas,
          dbplyr::in_schema("features",'crimenes_tasas_estados'),
          temporary = FALSE, overwrite = TRUE)
  dbDisconnect(con)

  print('Features written to: features.crimenes_tasas')
}
