#!/usr/bin/env Rscript
library(optparse)
library(dbplyr)
library(dplyr)
library(DBI)
library(yaml)
library(aws.s3)

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
  # Geometría Municipal

  query0 <- 'SELECT \'geom_municipios\' nivel,
                    cve_muni as clave, nom_muni as nombre, ST_AsGeoJSON( wkt )  as wkt
                FROM raw.geoms_municipios
                WHERE data_date=\'2018-a\''


  # Geometría Uniones Municipal en agregado estatal
  query1 <- 'SELECT \'geom_estados_municipios\' nivel,
                cve_ent as clave,
                ST_AsGeoJSON( ST_collect( f.wkt)) as wkt
                FROM raw.geoms_municipios As f
                WHERE data_date=\'2018-a\'
                GROUP BY cve_ent'

  # Geometría Agregados Estatales
  query2 <- 'SELECT \'geom_estados\' nivel,
              cve_ent as clave, nom_ent as nombre,
              ST_AsGeoJSON( f.wkt ) as wkt
              FROM raw.geoms_estados as f'

  # Query Tables
  geom_estados <- tbl(con, sql(query2)) %>%
      mutate(nivel = "geoms_estados") %>%
      collect()
  geom_municipios <- tbl(con, sql(query0)) %>%
      mutate(nivel = "geom_municipios") %>%
      collect()
  geom_mun_estados <- tbl(con, sql(query1)) %>%
      mutate(nivel = "geom_estados_municipios") %>% collect() %>%
      left_join(select(geom_estados, clave, nombre), by="clave")

  geoms <- bind_rows(geom_estados, geom_municipios) %>%
      bind_rows(geom_mun_estados)

  # Upload Table
  copy_to(con, geoms,
          dbplyr::in_schema("features","geojson_mun_ent"),
          temporary = FALSE, overwrite = TRUE)

  geojson <- "SELECT json_build_object(
                'type', 'FeatureCollection',
                'crs',  json_build_object(
                    'type',      'name',
                    'properties', json_build_object(
                    'name', 'EPSG:4326')),
            'features', json_agg(
                json_build_object(
                    'type', 'Feature',
                    'geometry',   wkt,
                    'properties', json_build_object(
                        'clave', clave,
                        'nombre', nombre,
                        'nivel', nivel)
                )
            )) FROM features.geojson_mun_ent"

  geojson_file <- tbl(con, sql(geojson)) %>% collect()

  s3save(toString(geojson_file), bucket = "sedesol-open-data",
         object = "geometrias_test.geojson")

  dbDisconnect(con)

  print('Features written to: features.geojson_mun_ent')
}
