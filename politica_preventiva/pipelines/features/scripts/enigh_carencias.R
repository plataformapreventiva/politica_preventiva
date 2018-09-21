#!/usr/bin/env Rscript
library(optparse)
library(tidyverse)
library(dbplyr)
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
              help="database host name", metavar="character"),
  make_option(c("--features_task"), type="character", default="",
              help="pipeline taks", metavar="character")
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

  con <- DBI::dbConnect(RPostgres::Postgres(),
                        host = PGHOST,
                        port = PGPORT,
                        dbname = PGDATABASE,
                        user = POSTGRES_USER,
                        password = POSTGRES_PASSWORD
  )


  source("funciones_auxiliares.R")

  # Read all tables
  hogares <- tbl(con, dbplyr::in_schema('clean', 'enigh_hogares'))
  poblacion <- tbl(con, dbplyr::in_schema('clean', 'enigh_poblacion'))
  viviendas <- tbl(con, dbplyr::in_schema('clean', 'enigh_viviendas'))
  trabajos <- tbl(con, dbplyr::in_schema('clean', 'enigh_trabajos'))
  ingresos <- tbl(con, dbplyr::in_schema('clean', 'enigh_ingresos'))

  # poblacion with year
  poblacion <- poblacion %>%
    filter(parentesco != 12)

  # Join to get the minimum age
  hog_pob <- poblacion %>% group_by(data_date, fecha_creacion, folioviv, hogar_id) %>%
    summarise(min_edad = min(edad, na.rm=TRUE)) %>%
    left_join(hogares, type = "inner", by = c("data_date","fecha_creacion","folioviv","hogar_id"))

  # Join poblacion and trabajo
  pob_trabajo <- left_join(poblacion, trabajos,
                           by = c("folioviv","hogar_id", "person_id","data_date", "fecha_creacion"))

  # Join edad and ingreso
  ingreso_edad <- poblacion %>% select(c('folioviv','hogar_id','person_id','data_date','fecha_creacion','edad')) %>%
    left_join(ingresos, type='left', by = c("folioviv","hogar_id", "person_id","data_date",'fecha_creacion'))

  #---------------------------------------
  # Carencias
  features <- make_features('carencias.yaml')

  # Rezago educativo
  print('Rezago Educativo:')
  vars = c('nivel_edu', 'a_nacimiento','ic_rezago_educativo')
  print(vars)
  ids = c('fecha_creacion','folioviv','hogar_id','person_id')
  rezago_educativo <- features(df = poblacion,
                              chunkname = 'rezago_educativo',
                              vars = vars,
                              ids = ids, return.all = FALSE) %>% compute()

  # Carencia Alimentaria
  print('Carencia Alimentaria:')
  vars = c('i_menores18', 'total_ia', 'inseguridad_alimentaria', 'ic_alimentacion')
  print(vars)
  ids = c('fecha_creacion','folioviv','hogar_id')
  carencias_hogares <- features(df = hog_pob,
                               chunkname = 'carencia_alimentaria',
                               vars = vars,
                               ids = ids, return.all = FALSE) %>% compute()

  # Carencia por Acceso a los Servicios Básicos en la Vivienda
  print('Carencia por Acceso a los Servicios Básicos en la Vivienda:')
  vars = c('ic_servicio_agua','ic_servicio_drenaje','ic_servicio_electricidad',
           'ic_combustible', 'ic_servicios_basicos')
  print(vars)
  ids = c('fecha_creacion','folioviv')
  carencias_servicios <- features(df = viviendas,
                                chunkname = 'carencia_servicios',
                                vars = vars,
                                ids = ids, return.all = FALSE) %>% compute()

  # Carencia en Calidad y Espacios en la Vivienda
  print('Carencia en Calidad y Espacios en la Vivienda:')
  vars = c('indice_hacinamiento', 'ic_hacinamiento', 'ic_material_piso',
           'ic_material_muros', 'ic_material_techos', 'ic_vivienda')
  print(vars)
  ids = c('fecha_creacion','folioviv')
  carencias_vivienda <- features(df = viviendas,
                                chunkname = 'carencia_vivienda',
                                vars = vars,
                                ids = ids, return.all = FALSE)  %>% compute()

  # tipo de trabajo
  print('Tipo de trabajo:')
  vars = c('pea','tipo_trab','jubilado')
  print(vars)
  ids = c('fecha_creacion','folioviv','hogar_id','person_id')
  tipo_trabajo <- features(df = pob_trabajo,
                      chunkname = 'trabajo_enigh',
                      vars = vars,
                      ids = ids, return.all = TRUE) %>% compute()

  # Carencia por acceso a servicios de salud
  print('Carencia por acceso a servicios de salud:')
  vars = c('sm_lab', 'sm_cv', 'salud_dir','jefe_sm','cony_sm','hijo_sm',
          'acceso_jefe_sm','acceso_cony_sm','acceso_hijo_sm','acceso_otros_sm',
          'asalud','ic_asalud')
  print(vars)
  ids = c('fecha_creacion','folioviv','hogar_id','person_id')
  carencias_salud <- features(df = tipo_trabajo,
                             chunkname = 'carencia_salud',
                             vars = vars,
                             ids = ids, return.all = FALSE) %>% compute()
  carencias_salud <- carencias_salud %>%
    group_by(!!! syms(ids)) %>%
      summarise_at( vars, funs(max(.,na.rm = TRUE))) %>% compute()

  # Carencia por acceso a seguridad social
  print('Ingresos:')
  vars <- c('ingreso_pam_temp','ingreso_pam','pam',
            'ingreso_pens_temp','ingreso_pens')
  print(vars)
  ids <- c('fecha_creacion','folioviv','hogar_id','person_id')
  ingresos_enigh <- features(df = ingreso_edad,
                       chunkname = 'ingresos_enigh',
                       vars = vars,
                       ids = ids, return.all = FALSE) %>% compute()

  ingresos_enigh <- ingresos_enigh %>%
    group_by(!!! syms(ids)) %>%
    summarise_at( vars, funs(max(.,na.rm = TRUE))) %>% compute()

  # Join ingreso_pam with trabajo
  trab_pam <- tipo_trabajo %>%
    left_join(ingresos_enigh,  type = "left", by = c("fecha_creacion","folioviv","hogar_id", "person_id"))

  # seguridad social
  print('Seguridad Social:')
  vars <- c('sm_lab','jubilado','sm_cv','afore_cv','ss_dir','jefe_ss','cony_ss',
           'hijo_ss','acceso_jefe_ss','acceso_cony_ss','acceso_hijo_ss',
           'acceso_otros_ss','seguridad_social','ic_seguridad_social')
  print(vars)
  ids <- c('fecha_creacion','folioviv','hogar_id','person_id')
  carencias_ss <- features(df = trab_pam,
                          chunkname = 'carencia_seguridad_social',
                          vars = vars,
                          id = ids, return.all = FALSE) %>% compute()

  carencias_ss <- carencias_ss %>%
    group_by(!!! syms(ids)) %>%
    summarise_at( vars, funs(max(.,na.rm = TRUE))) %>% compute()

  ### Join all
  print('Join all!')
  carencias_persona <- rezago_educativo %>%
    left_join(carencias_hogares, type = "left", by = c("fecha_creacion","folioviv","hogar_id")) %>%
    left_join(carencias_salud, type = "left", by = c("fecha_creacion","folioviv","hogar_id", "person_id")) %>%
    left_join(carencias_ss, type = "left", by = c("fecha_creacion","folioviv","hogar_id", "person_id")) %>%
    left_join(carencias_servicios, type = "left", by = c("fecha_creacion","folioviv")) %>%
    left_join(carencias_vivienda, type = "left", by = c("fecha_creacion","folioviv"))

  print('Copy!')
  copy_to(con, carencias_persona,
          dbplyr::in_schema("features",'enigh_carencias'),
          temporary = FALSE, overwrite = TRUE)
  dbDisconnect(con)

}
