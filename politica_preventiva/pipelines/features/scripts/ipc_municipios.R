#!/usr/bin/env Rscript
library(dbplyr)
library(dplyr)
library(DBI)
library(dotenv)
library(geosphere)
library(lwgeom)
library(mapsapi)
library(optparse)
library(sf)
library(sp)


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
  dotenv::load_dot_env()

  # Obtain inflation data by querying and mutating prices data

  ipc_ciudades <- dplyr::tbl(con, dbplyr::in_schema('clean', 'ipc_ciudades')) %>%
                    dplyr::collect()

  inflacion_ciudades <- tbl(con, dbplyr::in_schema('clean', 'ipc_ciudades')) %>%
                          dplyr::collect() %>%
                          dplyr::group_by(ciudad) %>%
                          dplyr::arrange(year, month) %>%
                          dplyr::mutate_at(vars(indice:scian_3), as.numeric) %>%
                          dplyr::mutate_at(vars(indice:scian_3),
                                           funs(dif = (.-lag(.))/.)) %>%
                          dplyr::ungroup() %>%
                          tidyr::unite('yearmon', c('year', 'month'))

  periods <- inflacion_ciudades %>%
              dplyr::pull(yearmon) %>%
              unique() %>%
              sort(decreasing = TRUE)

  inflacion_ciudades_lp <- inflacion_ciudades %>%
                            dplyr::filter(yearmon == periods[1])

  # Obtain and standardize georef data sources
  geoms_municipios <- tbl(con, dbplyr::in_schema('geoms', 'municipios')) %>%
                        dplyr::collect()

  ciudades <- inflacion_ciudades_lp %>%
                dplyr::pull(ciudad) %>%
                mapsapi::mp_geocode(key = Sys.getenv('key')) %>%
                mapsapi::mp_get_points()
  n_cities <- nrow(ciudades)

  municipios_latlon <- geoms_municipios %>%
                        dplyr::select(latitud, longitud)
  sp::coordinates(municipios_latlon) <- c("longitud", "latitud")
  sp::proj4string(municipios_latlon) <- sp::CRS(sf::st_crs(ciudades)$proj4string)
  municipios_points <- sf::st_as_sf(municipios_latlon, c('longitud', 'latitud'))

  # Calculate needed distances and obtain city weights for each municipality
  distance_matrix <- sf::st_distance(municipios_points$geometry, ciudades$pnt)
  distance_matrix[] <- vapply(distance_matrix, function(x){1/x}, numeric(1))
  row_denominator <- distance_matrix %*% rep(1, n_cities) %>%
                        vapply(., function(x){1/x}, numeric(1))
  weights_matrix <- replicate(n_cities, row_denominator) * distance_matrix

  print('Successfully computed weights matrix')

  inflacion_municipios <- inflacion_ciudades_lp %>%
                            dplyr::select(ends_with('dif')) %>%
                            purrr::map_df(function(x) weights_matrix %*% x) %>%
                            dplyr::bind_cols(dplyr::select(geoms_municipios, cve_muni), .)

  dplyr::copy_to(con, inflacion_municipios,
                 dbplyr::in_schema("features",'inflacion_municipios'),
                 temporary = FALSE, overwrite = TRUE)
  dbDisconnect(con)

  print('Features written to: features.inflacion_municipios')
}

