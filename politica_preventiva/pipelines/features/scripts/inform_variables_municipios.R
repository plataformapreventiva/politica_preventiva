#!/usr/bin/env Rscript
library(optparse)
library(dbplyr)
library(dplyr)
library(DBI)
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
  
  #----------------------------------------------------------------------------------------
  
  print('Pulling datasets')
  
  # AMENZAZAS
  cenapred <- tbl(con, dbplyr::in_schema('clean','cenapred_app_municipios'))%>%
    select(cve_muni,gp_inundac,gp_sequia2,gp_bajaste,gp_ciclnes,g_suscelad,gp_sismico,gp_tsunami)
  crimenes_tasas <- tbl(con, dbplyr::in_schema('features','crimenes_tasas')) %>%
    select(cve_muni,homicidio_culposo_tasa,homicidio_doloso_tasa,feminicidio_tasa,
           secuestro_tasa,robo_vehiculos_tasa,violencia_familiar_tasa) %>%
    group_by(cve_muni) %>%
    summarise(homicidio_culposo_tasa=avg(homicidio_culposo_tasa),
              homicidio_dolos_tasao=avg(homicidio_doloso_tasa),
              feminicidio_tasa=avg(feminicidio_tasa),
              secuestro_tasa=avg(secuestro_tasa),
              robo_vehiculos_tasa=avg(robo_vehiculos_tasa),
              violencia_familiar_tasa=avg(violencia_familiar_tasa))
  intercensal_amenazas <- tbl(con, dbplyr::in_schema('features','intercensal_personas_2015')) %>%
    select(cve_muni,tasa_flot_esc,tasa_flot_trab,migracion_interna_reciente,migracion_externa_reciente)
  
  amenazas <- left_join(cenapred,crimenes_tasas, by='cve_muni') %>%
    left_join(intercensal_amenazas, by='cve_muni')
  
  #----------------------------------------------------------------------------------------
  
  # CAPACIDADES
  query <- 'SELECT LPAD(cve_muni::text, 5, \'0\') as cve_muni, 
  nd_spnn1, nd_nsnn4
  FROM clean.cngmd_proteccion_civil'
  riesgo <- tbl(con, sql(query)) 
  query2 <- 'SELECT LPAD(cve_muni::text, 5, \'0\') as cve_muni,
  cobertura_dren
  FROM clean.cngmd_drenaje'
  drenaje <- tbl(con, sql(query2)) 
  query3 <- 'SELECT LPAD(cve_muni::text, 5, \'0\') as cve_muni,
  basura_porcent_cab
  FROM clean.cngmd_recoleccion_basura'
  recoleccion_basura <- tbl(con, sql(query3))
  query4 <- 'SELECT LPAD(cve_muni::text, 5, \'0\') as cve_muni,
  aut_cobr, totalpo1
  FROM clean.cngmd_predial'
  predial <- tbl(con, sql(query4)) 
  query5 <- 'SELECT LPAD(cve_muni::text, 5, \'0\') as cve_muni,
  nd_nsnn1
  FROM clean.cngmd_participacion_ciudadana'
  participacion_ciudadana <- tbl(con, sql(query5)) 
  cfe <- tbl(con, dbplyr::in_schema('clean','cfe')) 
  hospitales <- tbl(con, dbplyr::in_schema('features','recursos_hospitales')) %>% 
    select(-c(actualizacion_sedesol,data_date))
  internet <- tbl(con, dbplyr::in_schema('raw','internet_municipios')) %>% 
    select(cve_muni,pct_viv_con_internet)
  complejidad <- tbl(con, dbplyr::in_schema('raw','complejidad_municipios')) %>% 
    select(cve_muni, complejidad_2014)
  indice_reglamentacion <- tbl(con, dbplyr::in_schema('raw','indice_reglamentacion_municipios')) %>% 
    filter(anio == 2014) %>%
    select(cve_muni,indice)
  indicadores_finanzas <- tbl(con, dbplyr::in_schema('raw','indicadores_financieros_municipios')) %>% 
    filter(anio == 2014) %>%
    select(cve_muni,flexibilidad_financiera,capacidad_inversion)
  transparencia <- tbl(con, dbplyr::in_schema('clean','cngmd_transparencia')) %>% 
    select(-ubic_geo)
  mortalidad <- tbl(con, dbplyr::in_schema('features','mortalidad_tasas'))
  
  capacidades <- left_join(riesgo,indice_reglamentacion, by='cve_muni') %>%
    # left_join(transparencia, by='cve_muni') %>%  #corregir
    # left_join(participacion_ciudadana, by='cve_muni') %>% #corregir
    left_join(indicadores_finanzas, by='cve_muni') %>% 
    left_join(predial, by='cve_muni') %>% 
    left_join(complejidad, by='cve_muni') %>%
    left_join(cfe, by='cve_muni') %>% 
    left_join(internet, by='cve_muni') %>% 
    left_join(drenaje,by='cve_muni') %>% 
    left_join(recoleccion_basura,by='cve_muni') %>% 
    left_join(hospitales,by='cve_muni') %>% 
    left_join(mortalidad,by='cve_muni')
  
  #----------------------------------------------------------------------------------------
  
  # VULNERABILIDADES
  coneval <- tbl(con, dbplyr::in_schema('clean','coneval_municipios')) %>%   
    filter(data_date == "2015-a") %>% 
    select(cve_muni, ic_rezedu_porcentaje, ic_asalud_porcentaje, ic_segsoc_porcentaje, ic_cv_porcentaje,
           vul_ing_porcentaje, ic_sbv_porcentaje, pobreza_porcentaje)
  intercensal_vulnerabilidades <- tbl(con, dbplyr::in_schema('features','intercensal_personas_2015')) %>%
    select(cve_muni,brecha_tasa_ocupados,brecha_horas_cuidados,
           prop_mayores65,prop_menores5, prop_adultos, prop_menores, 
           prop_indigenas,prop_lengua_indigena,prop_afrodescendientes,
           prop_mujeres,prop_hombres)
  # Falta Relación de dependencia, corregir features
  # Número de personas en edades teóricamente inactivas (personas de 0 a 14 y de 65 años y más) 
  # por cada cien personas en edades teóricamente activas (personas de 15 a 64 años).
  intercensal_viviendas <- tbl(con, dbplyr::in_schema('features','intercensal_viviendas_2015'))
  
  vulnerabilidades <- left_join(coneval, intercensal_vulnerabilidades, by='cve_muni') %>%
    left_join(intercensal_viviendas, by='cve_muni') %>% 
    select(-c(actualizacion_sedesol,data_date))
  
  
  #----------------------------------------------------------------------------------------
  
  inform_mun <- left_join(amenazas,vulnerabilidades, by= 'cve_muni') %>%
    left_join(capacidades, by='cve_muni') %>% 
    dplyr::mutate(data_date=data_date,
                  actualizacion_sedesol = lubridate::today())
  
  copy_to(con, inform_mun,
          dbplyr::in_schema("features",'inform_variables_municipios'),
          temporary = FALSE, overwrite = TRUE)
  dbDisconnect(con)
  
  print('Features written to: features.inform_variables_municipios')
}
