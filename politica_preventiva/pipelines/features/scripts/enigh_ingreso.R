#!/usr/bin/env Rscript
library(optparse)
library(tidyverse)
library(dbplyr)
library(DBI)
library(yaml)

option_list = list(
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
  concentrados <- tbl(con, dbplyr::in_schema('clean', 'enigh_concentrados'))

  # poblacion with year
  poblacion <- poblacion %>%
    filter(parentesco != 12)

  # Join poblacion and trabajo
  pob_trabajo <- left_join(poblacion, trabajos,
                           by = c("folioviv","hogar_id", "person_id","data_date","fecha_creacion"))

  # join Poblacion vivienda
  hogares_ids <- hogares %>% select(hogar_id, folioviv, fecha_creacion)
  viviendas_sub <- viviendas %>% select(folioviv, fecha_creacion, total_cuartosdor, total_personas)
  concentrados_sub <- concentrados %>% select(hogar_id, folioviv, fecha_creacion, remesas)
  pob_viv <- poblacion  %>%
      left_join(hogares_ids, by = c("folioviv","hogar_id","fecha_creacion")) %>%
             left_join(viviendas_sub, by = c("folioviv","fecha_creacion")) %>%
             left_join(concentrados_sub, by = c("folioviv", "hogar_id", "fecha_creacion")) %>% compute()

  # Join Vivienda y hogar
  viv_hog <- left_join(viviendas, hogares,
                       by = c("folioviv","data_date","fecha_creacion"))

  # Ubicaciones
  ubicaciones <- viviendas %>% mutate(cve_loc = ubica_geo) %>%
    select(folioviv, cve_loc, fecha_creacion) %>% compute()
  # Ingreso corriente
  ingresos_corrientes <- concentrados %>%
    select(folioviv, hogar_id, ing_cor, fecha_creacion) %>% compute()

  # ------------------------------------------------
  features <- make_features('ingreso.yaml')

  # Educacion
  print('Educacion')
  vars <- c('jefe_escolaridad', 'p_esc3', 'p_esc5','num_asiste_escuela_5a15')
  print(vars)
  ids <- c('fecha_creacion','folioviv','hogar_id')
  educacion <- features(df = poblacion,
                        chunkname = 'educacion',
                        vars = vars,
                        ids = ids, return.all = FALSE) %>% compute()
  educacion <- educacion %>%
    group_by(!!! syms(ids)) %>%
    summarise_at( vars, funs(max(.,na.rm = TRUE))) %>% compute()

  # Condicion laboral
  print('Condicion laboral')
  vars <- c('jefe_edad','num_trabaja_subor','num_trabaja_indep','jefe_subord',
           'jefe_indep','num_trabaja_sin_pago','num_hombres_trabajan',
           'num_mujeres_trabajan', 'hombres_trabajan_excl',
           'num_menores_trabaja','num_adol_trabaja',
           'num_trabajan','num_no_trabajan','depen_cociente','num_con_sueldo')
  print(vars)
  ids <- c('fecha_creacion','folioviv','hogar_id')
  condicion_laboral <- features(df = pob_trabajo,
                                chunkname = 'condicion_laboral',
                                vars = vars,
                                ids = ids, return.all = FALSE) %>% compute()
  condicion_laboral <- condicion_laboral %>%
    group_by(!!! syms(ids)) %>%
    summarise_at( vars, funs(max(.,na.rm = TRUE))) %>% compute()

  # Seguridad Social
  print('Seguridad Social')
  vars <- c('con_servicios_salud','num_seguro_popular', 'con_seguro_popular','ssjtrabind')
  print(vars)
  ids <- c('fecha_creacion','folioviv','hogar_id')
  seguridad_social <- features(df = pob_trabajo,
                               chunkname = 'seguridad_social',
                               vars = vars,
                               ids = ids, return.all = FALSE) %>% compute()
  seguridad_social <- seguridad_social %>%
    group_by(!!! syms(ids)) %>%
    summarise_at( vars, funs(max(.,na.rm = TRUE))) %>% compute()

  # Condiciones de Vivienda
  print('Condiciones de Vivienda')
  vars <- c('piso_tierra','piso_firme', 'piso_rec',
            'techo_blando','pared_blando','sin_agua','sin_basurero',
           'sin_sanitario','sanitario_cagua_excl',
           'sin_combustible','viv_propia_integrante','tenencia_propia','viv_renta',
           'total_cuartosdor','int_techo_piso')
  print(vars)
  ids <- c('fecha_creacion','folioviv')
  condiciones_vivienda <- features(df = viviendas,
                                   chunkname = 'condiciones_vivienda',
                                   vars = vars,
                                   ids = ids, return.all = FALSE) %>% compute()

  # Condiciones demograficas
  print('Condiciones demograficas')
  vars <- c('total_personas','unipersonales','log_total_personas',
            'cuartos_dormir','hacinamiento')
  ids <- c('fecha_creacion','folioviv','hogar_id')
  condiciones_demograficas1 <- features(df = pob_viv ,
                                       chunkname = 'condiciones_demograficas1',
                                       vars = vars,
                                       ids = ids, return.all = FALSE) %>% compute()
  condiciones_demograficas1 <- condiciones_demograficas1 %>%
    group_by(!!! syms(ids)) %>%
    summarise_at( vars, funs(max(.,na.rm = TRUE))) %>% compute()

  vars <- c('num_dependientes','num_independientes',
            'dependientes_cociente','num_mujeres','num_hombres',
            'mas_mujeres', 'solo_mujeres','num_mujeres_15a49',
            'num_edad0a5', 'con_edad0a5', 'num_adultos_mayores',
            'num_exc_lengua_indigena','exc_lengua_indigena','jefe_mayor40',
            'jefatura_femenina','recibe_remesas', 'con_remesa','c_discapacidad')
  condiciones_demograficas2 <- features(df = pob_viv ,
                                        chunkname = 'condiciones_demograficas2',
                                        vars = vars,
                                        ids = ids, return.all = FALSE) %>% compute()
  condiciones_demograficas2 <- condiciones_demograficas2 %>%
    group_by(!!! syms(ids)) %>%
    summarise_at( vars, funs(max(.,na.rm = TRUE))) %>% compute()

  # Enseres del hogar
  print('Enseres del hogar')
  vars <- c('refrigerador','refrigerador_sirve','ts_refrigerador','sin_refrigerador',
            'vehiculo','vehiculo_sirve','ts_vehiculo','sin_vehiculo','videocas','videocas_sirve',
            'ts_videocas','sin_videocas','computadora','computadora_sirve','ts_computadora',
            'sin_computadora','horno','horno_sirve','ts_horno','sin_horno','lavadora',
            'lavadora_sirve','ts_lavadora','sin_lavadora','estufa','estufa_sirve','ts_estufa',
            'sin_estufa','telefono_sirve','ts_telefono','sin_telefono','int_videocas_horno')
  print(vars)
  ids <- c('fecha_creacion','folioviv','hogar_id')
  enseres_hogar <- features(df = viv_hog ,
                            chunkname = 'enseres_hogar',
                            vars = vars,
                            ids = ids, return.all = FALSE) %>% compute()
  enseres_hogar <- enseres_hogar %>%
    group_by(!!! syms(ids)) %>%
    summarise_at( vars, funs(max(.,na.rm = TRUE))) %>% compute()

  # Seguridad Alimentaria
  print('Seguridad Alimentaria')
  vars <- c('inseg_alim','sin_alim','seg_alim_a')
  print(vars)
  ids <- c('fecha_creacion','folioviv','hogar_id')
  seguridad_alimentaria <- features(df = hogares,
                                    chunkname = 'seguridad_alimentaria',
                                    vars = vars,
                                    ids = ids, return.all = FALSE) %>% compute()
  ### Join all
  print('Join all!')
  features_hogares <- educacion %>%
    left_join(seguridad_social, type = "left", by = c("fecha_creacion","folioviv","hogar_id")) %>%
    left_join(condicion_laboral, type = "left", by = c("fecha_creacion","folioviv","hogar_id")) %>%
    left_join(condiciones_vivienda, type = "left", by = c("fecha_creacion","folioviv")) %>%
    left_join(condiciones_demograficas1, type = "left", by = c("fecha_creacion","folioviv","hogar_id")) %>%
    left_join(condiciones_demograficas2, type = "left", by = c("fecha_creacion","folioviv","hogar_id")) %>%
    left_join(enseres_hogar, type = "left", by = c("fecha_creacion","folioviv","hogar_id")) %>%
    left_join(seguridad_alimentaria, type = "left", by = c("fecha_creacion","folioviv","hogar_id")) %>%
    left_join(ingresos_corrientes, type = "left", by = c("fecha_creacion", "folioviv", "hogar_id")) %>%
    left_join(ubicaciones, type='left', by=c('fecha_creacion','folioviv'))

  print('Copy!')
  copy_to(con, features_hogares,
          dbplyr::in_schema("features",'enigh_ingreso'),
          temporary = FALSE, overwrite = TRUE)
  dbDisconnect(con)

}
