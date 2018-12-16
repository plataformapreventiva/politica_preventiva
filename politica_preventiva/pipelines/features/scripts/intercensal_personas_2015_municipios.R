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

  print('Pulling datasets')

  query <- 'SELECT *, LPAD(ent_pais_asi::text, 2, \'0\') || LPAD(mun_asi::text, 3, \'0\') as cve_muni_esc,
                   LPAD(ent_pais_trab::text, 2, \'0\') || LPAD(mun_trab::text, 3, \'0\') as cve_muni_trab
                                    FROM clean.intercensal_personas_2015'

  intercensal <- tbl(con, sql(query))

  #Población por municipio y grupos
  pob_mun <- intercensal %>% select(cve_muni,poblacion_mun,mayores65, menores5, adultos, menores,
                                    indigenas, lengua_indigena, afrodescendientes, mujeres, hombres) %>%
    group_by(cve_muni) %>% summarise_all(sum) %>%
    mutate(prop_mayores65= mayores65/poblacion_mun,
           prop_menores5= menores5/poblacion_mun,
           prop_adultos= adultos/poblacion_mun,
           prop_menores= menores/poblacion_mun,
           prop_indigenas= indigenas/poblacion_mun,
           prop_lengua_indigena= lengua_indigena/poblacion_mun,
           prop_afrodescendientes= afrodescendientes/poblacion_mun,
           prop_mujeres= mujeres/poblacion_mun,
           prop_hombres= hombres/poblacion_mun)

  #MIGRACION INTERNA Y EXTERNA RECIENTE
  migracion_reciente <- intercensal %>%
    mutate(migracion_interna_reciente = as.double(migrantes_internos_recientes)/poblacion_mun) %>%
    mutate(migracion_externa_reciente = as.double(migrantes)/poblacion_mun) %>%
    select(cve_muni,migracion_interna_reciente,migracion_externa_reciente) %>%
    group_by(cve_muni) %>% summarise_all(mean,na.rm = TRUE)

  #POBLACION FLOTANTE

  #Población flotante por municipio debido a la escuela
  poblacion_flotante_esc <- intercensal %>%
    mutate(pob_flot_esc = ifelse(cve_muni_esc == cve_muni, 0, 1)) %>%
    group_by(cve_muni) %>% summarise(pob_flot_esc = sum(pob_flot_esc,na.rm = TRUE)) %>%
    left_join(pob_mun, by='cve_muni') %>%
    mutate(tasa_flot_esc = pob_flot_esc/poblacion_mun)  %>%
    select(cve_muni,pob_flot_esc,tasa_flot_esc)

  #Población flotante por municipio debido al trabajo
  poblacion_flotante_trab <- intercensal %>%
    mutate(pob_flot_trab = ifelse(cve_muni_trab == cve_muni, 0, 1)) %>%
    group_by(cve_muni) %>% summarise(pob_flot_trab = sum(pob_flot_trab,na.rm = TRUE)) %>%
    left_join(pob_mun, by='cve_muni') %>%
    mutate(tasa_flot_trab = pob_flot_trab/poblacion_mun)  %>%
    select(cve_muni,pob_flot_trab,tasa_flot_trab)

  # BRECHA TIEMPO DE CUIDADOS
  horas <- intercensal %>%
    select(cve_muni,sexo,acti_sin_pago1,acti_sin_pago2,
           acti_sin_pago3,acti_sin_pago4,acti_sin_pago5,acti_sin_pago6,
           acti_sin_pago7,acti_sin_pago8)  %>%
    mutate(total_horas = (acti_sin_pago1 + acti_sin_pago2 +
                            acti_sin_pago3 + acti_sin_pago4 + acti_sin_pago5 +
                            acti_sin_pago6 + acti_sin_pago7 + acti_sin_pago8)) %>%
    group_by(cve_muni,sexo) %>% summarise_all(mean,na.rm = TRUE)

  hombres <- horas %>%
    filter(sexo == 1)  %>% mutate(total_horas_h=total_horas) %>% select(-c(sexo,total_horas))

  mujeres <- horas %>%
    filter(sexo == 3) %>% mutate(total_horas_m=total_horas) %>% select(-c(sexo,total_horas))

  brecha_horas_cuidados <- left_join(hombres, mujeres, by = 'cve_muni') %>%
    mutate(brecha_horas_cuidados = ifelse(total_horas_h == 0,total_horas_m,total_horas_m/total_horas_h)) %>%
    select(cve_muni, brecha_horas_cuidados,total_horas_h,total_horas_m)

# HOMBRES Y MUJERES EN EL MERCADO LABORAL
  ocupadas <- intercensal %>%
    filter(sexo == 3) %>% mutate(edad_trabajar = ifelse(edad > 13, 1, 0),
           ocupadas = ifelse(conact %in% c(10,11,12,13,14,15,16), 1,0)) %>%
    mutate(edad_trabajar_f = edad_trabajar*as.numeric(factor),
           ocupadas_f = ocupadas*as.numeric(factor)) %>%
    select(cve_muni,ocupadas_f,edad_trabajar_f) %>%
    replace(is.na(.), 0) %>% group_by(cve_muni)  %>% summarise_all(sum) %>%
    mutate(proporcion_mujeres_ocupadas = ocupadas_f/edad_trabajar_f) %>%
    select(cve_muni, proporcion_mujeres_ocupadas)

  ocupados <- intercensal %>%
    filter(sexo == 1) %>%
    mutate(edad_trabajar = ifelse(edad > 13, 1, 0),
           ocupados = ifelse(conact %in% c(10,11,12,13,14,15,16), 1,0)) %>%
    mutate(edad_trabajar_f = edad_trabajar*as.numeric(factor),
           ocupados_f = ocupados*as.numeric(factor)) %>%
    select(cve_muni,ocupados_f,edad_trabajar_f) %>%
    group_by(cve_muni)  %>% summarise_all(sum) %>%
    mutate(proporcion_hombres_ocupados = ocupados_f/edad_trabajar_f) %>%
    select(cve_muni, proporcion_hombres_ocupados)

  brecha_tasa_ocupados <- left_join(ocupadas,ocupados, by='cve_muni') %>%
    mutate(brecha_tasa_ocupados = ifelse(proporcion_hombres_ocupados == 0,proporcion_mujeres_ocupadas,proporcion_mujeres_ocupadas / proporcion_hombres_ocupados)) %>%
    select(cve_muni, brecha_tasa_ocupados,proporcion_hombres_ocupados,proporcion_mujeres_ocupadas)

  # PIRÁMIDE POBLACIONAL
  replace_high_bound <- function(x){
      wrong_bound <- gsub('.*,(.*)\\)', '\\1', x) %>% as.numeric()
      paste0(gsub('(.*,)(.*)\\)', '\\1', x), wrong_bound-1, ')')
  }

  cut_edad <- function(x){
    cut(x, breaks = c(seq(from = 0, to = 76, by = 5), Inf), right = F) %>%
        replace_high_bound() %>%
        gsub('\\[([0-9]+),(.*)\\)', '\\1-\\2', .) %>%
        gsub('-Inf', '+', .)
  }

  piramide_pob <- intercensal %>% collect() %>%
                dplyr::mutate(grupo_edad = cut_edad(edad),
                              sexo = recode(sexo, `1` = 'h', `3` = 'm'),
                              grupo_pob = paste0(sexo, grupo_edad)) %>%
                dplyr::group_by(cve_muni, grupo_pob) %>%
                dplyr::summarise(total_personas = sum(poblacion_mun)) %>%
                dplyr::select(variable = grupo_pob,
                              valor = total_personas) %>%
                dplyr::ungroup() %>%
                spread(variable, valor) %>%
                dplyr::rename()

  # JOIN
  intercensal_personas_2015 <- left_join(poblacion_flotante_esc,poblacion_flotante_trab, by= 'cve_muni') %>%
    left_join(migracion_reciente, by='cve_muni') %>%
    left_join(brecha_tasa_ocupados, by='cve_muni') %>%
    left_join(brecha_horas_cuidados, by='cve_muni') %>%
    left_join(pob_mun, by='cve_muni') %>% collect() %>%
    left_join(piramide_pob, by='cve_muni') %>%
    dplyr::mutate(actualizacion_sedesol = lubridate::today())
  colnames(intercensal_personas_2015) <- gsub("-", "_", colnames(intercensal_personas_2015))
  colnames(intercensal_personas_2015) <- gsub("+", "", colnames(intercensal_personas_2015))

  copy_to(con, intercensal_personas_2015,
          dbplyr::in_schema("features",'intercensal_personas_2015_municipios'),
          temporary = FALSE, overwrite = TRUE)
  dbDisconnect(con)

  print('Features written to: features.intercensal_personas_2015_municipios')
}
