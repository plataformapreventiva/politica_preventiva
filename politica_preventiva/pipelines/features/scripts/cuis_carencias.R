#!/usr/bin/env Rscript
library(optparse)
library(dbrsocial)
library(dbplyr)
library(dplyr)
library(DBI)
library(yaml)
#source("recode_utils.R")

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
  make_option(c("--pipeline_task"), type="character", default="",
              help="pipeline taks", metavar="character"),
  make_option(c("--scripts_dir"), type="character", default="",
              help="scripts directory", metavar="character")
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
                        user = PGHOST,
                        password = POSTGRES_PASSWORD
  )

  source("funciones_auxiliares.R")


############################################################################################
# Tablas LOCALES para tests
############################################################################################
#  se_vivienda_local <- sample_table(connec#tion = con,
                                   # schema #= 'clean',
                                   # the_table = 'cuis_historico_se_vivienda')
#
#  se_integrante_local <- sample_table(connection = con,
#                                      schema = 'clean',
#                                      the_table = 'cuis_historico_se_integrante')
#
#  edad_integrante_local <- sample_table(connection = con,
#                                        schema = 'clean',
#                                        the_table = 'cuis_historico_integrante') %>%
#                            dplyr::select(hogar_id, person_id, edad, anio, parentesco)
#
#  personas_vivienda_local <- sample_table(connection = con,
#                                          schema = 'clean',
#                                          the_table = 'cuis_historico_vivienda') %>%
#                             dplyr::select(hogar_id, total_personas)
#
#  df_integrante_local <- dplyr::left_join(se_integrante_local,
#                                          edad_integrante_local)
#
#  df_vivienda_local <- dplyr::left_join(se_vivienda_local,
#                                        personas_vivienda_local) %>%
#                       dplyr::select(-one_of(not_selected))
#
#  df_cuis_local <- dplyr::left_join(df_vivienda_local,
#                                               edad_integrante_local)
#
#  df_cuis_local = left_join(df_integrante_local, df_vivienda_local)
#
#  df_cuis_local_recoded = rename(df_cuis_local,
#                                 prestacion_incapacidad = trab_presta_a,
#                                 prestacion_afore = trab_presta_b,
#                                 segvol_medico = seg_volunt_b,
#                                 segvol_afore = seg_volunt_a)
#
  # Load YAML into core function
  carencias_chunk <- make_features('carencias.yaml')

  se_integrante <- large_table(connection = con,
                               schema = 'clean',
                               the_table = 'cuis_historico_se_integrante')

  edad_integrante <- large_table(connection = con,
                                 schema = 'clean',
                                 the_table = 'cuis_historico_integrante') %>%
                     dplyr::select(hogar_id, person_id, edad, anio, parentesco) %>%
                     dplyr::compute()

  personas_vivienda <- large_table(connection = con,
                             schema = 'clean',
                             the_table = 'cuis_historico_vivienda') %>%
                       dplyr::select(hogar_id, total_personas) %>%
                       dplyr::compute()

  not_selected <- c('llave_hogar_h', 'usr_creacion', 'csc_hogar',
                    'actualizacion_sedesol', 'data_date', 'fecha_creacion')


  df_integrante <- dplyr::left_join(se_integrante, edad_integrante) %>%
                   dplyr::compute()

  df_vivienda <- dplyr::left_join(se_vivienda, personas_vivienda) %>%
                 dplyr::select(-one_of(not_selected)) %>%
                 dplyr::compute()

  df_cuis <- left_join(df_vivienda, df_integrante) %>%
             dplyr::compute()
  # Rezago educativo
  print('Rezago Educativo')
  re_vars <- c('antec_esc', 'nivel_edu',
               'a_nacimiento', 'ic_rezago_educativo')
  carencia_educacion <- carencias_chunk(df = df_integrante,
                                        chunkname = 'rezago_educativo',
                                        vars = re_vars,
                                        ids = c('hogar_id', 'person_id')) %>%
                        dplyr::compute()

  print('Copy to features.cuis_carencia_educacion')
  copy_to(con, carencia_educacion,
          dbplyr::in_schema("features",'cuis_carencia_educacion'),
          temporary = FALSE, overwrite = TRUE)

  dbDisconnect(con)

  # Carencia Alimentaria
  con <- prev_connect()
  se_vivienda <- large_table(connection = con,
                               schema = 'clean',
                               the_table = 'cuis_historico_se_vivienda')

  edad_integrante <- large_table(connection = con,
                                 schema = 'clean',
                                 the_table = 'cuis_historico_integrante') %>%
                     dplyr::select(hogar_id, person_id, edad, anio, parentesco) %>%
                     dplyr::compute()

  df_alimentaria = left_join(se_vivienda, edad_integrante)

  print('Inseguridad Alimentaria')
  alimentaria_vars <- c('min_edad', 'i_menores18',
                 'total_ia', 'inseguridad_alimentaria',
                 'ic_alimentacion')
  carencia_alimentaria <- carencias_chunk(df = df_alimentaria,
                                          chunkname = 'carencia_alimentaria',
                                          vars = alimentaria_vars,
                                          ids = 'hogar_id') %>%
                          dplyr::compute()

  print('Copy to features.cuis_carencia_alimentaria')
  copy_to(con, carencia_alimentaria,
          dbplyr::in_schema("features",'cuis_carencia_alimentaria'),
          temporary = FALSE, overwrite = TRUE)

  dbDisconnect(con)

  # Carencia en Calidad y Espacios en la Vivienda
  con <- prev_connect()

  se_vivienda <- large_table(connection = con,
                             schema = 'clean',
                             the_table = 'cuis_historico_se_vivienda')

  personas_vivienda <- large_table(connection = con,
                             schema = 'clean',
                             the_table = 'cuis_historico_vivienda') %>%
                       dplyr::select(hogar_id, total_personas) %>%
                       dplyr::compute()

  df_vivienda <- dplyr::left_join(se_vivienda, personas_vivienda) %>%
                 dplyr::select(-one_of(not_selected)) %>%
                 dplyr::compute()

  print('Calidad y Espacios en la Vivienda')
  vivienda_vars <- c('indice_hacinamiento', 'ic_hacinamiento',
                  'ic_material_piso', 'ic_material_muros',
                  'ic_material_techos', 'ic_vivienda')
  carencia_vivienda <- carencias_chunk(df = df_vivienda,
                                       chunkname = 'carencia_vivienda',
                                       vars = vivienda_vars,
                                       ids = 'hogar_id') %>%
                        dplyr::compute()

  copy_to(con, carencia_vivienda,
          dbplyr::in_schema("features",'cuis_carencia_vivienda'),
          temporary = FALSE, overwrite = TRUE)

  dbDisconnect(con)

  # Carencia por Acceso a los Servicios Básicos en la Vivienda
  con <- prev_connect()

  df_servicios <- large_table(connection = con,
                              schema = 'clean',
                              the_table = 'cuis_historico_se_vivienda')


  print('Acceso a los Servicios Básicos en la Vivienda')
  servicios_vars <- c('ic_servicio_agua', 'ic_servicio_drenaje',
                      'ic_servicio_electricidad', 'ic_combustible',
                      'ic_servicios_basicos')
  carencia_servicios <- carencias_chunk(df = df_servicios,
                                        chunkname = 'carencia_servicios',
                                        vars = servicios_vars,
                                        ids = 'hogar_id') %>%
                        dplyr::compute()

  copy_to(con, carencia_servicios,
          dbplyr::in_schema("features",'cuis_carencia_servicios'),
          temporary = FALSE, overwrite = TRUE)

  dbDisconnect(con)

  # Carencia por Acceso a los Servicios de Salud
  con <- prev_connect()

  selected_trabajo <- c('person_id', 'hogar_id', 'realiza_act_lab',
                        'realiza_act_nolab', 'trabaja_subord', 'trabaja_indep',
                        'con_sueldo')

  df_integrante <- large_table(connection = con,
                               schema = 'clean',
                               the_table = 'cuis_historico_se_integrante') %>%
                   dplyr::select(one_of(selected_trabajo))

  edad_integrante <- large_table(connection = con,
                                 schema = 'clean',
                                 the_table = 'cuis_historico_integrante') %>%
                     dplyr::select(hogar_id, person_id, edad)

  df_trabajo <- left_join(df_integrante, edad_integrante) %>%
                dplyr::compute()

  print('Trabajo')
  trabajo_vars <- c('trabajo', 'pea', 'tipo_trab')
  trabajo_cuis <- carencias_chunk(df = df_trabajo,
                                  chunkname = 'trabajo_cuis',
                                  vars = trabajo_vars,
                                  ids = c('person_id', 'hogar_id')) %>%
                  dplyr::compute()

  copy_to(con, trabajo_cuis,
          dbplyr::in_schema("features",'cuis_trabajo'),
          temporary = FALSE, overwrite = TRUE)

  dbDisconnect(con)

  con <- prev_connect()

  not_selected <- c('llave_hogar_h', 'usr_creacion', 'csc_hogar',
                    'actualizacion_sedesol', 'data_date', 'fecha_creacion')

  selected_integrante <- c('person_id', 'hogar_id',
                           'c_instsal_a', 'c_instsal_b',
                           'c_afilsal_a', 'c_afilsal_b')

  se_integrante <- large_table(connection = con,
                               schema = 'clean',
                               the_table = 'cuis_historico_se_integrante') %>%
                   dplyr::select(one_of(selected_integrante))

  selected_vivienda <- c('hogar_id', 'c_saludhoga', 'c_saludhogb')

  se_vivienda <- large_table(connection = con,
                               schema = 'clean',
                               the_table = 'cuis_historico_se_vivienda') %>%
                   dplyr::select(one_of(selected_vivienda))

  edad_integrante <- large_table(connection = con,
                                 schema = 'clean',
                                 the_table = 'cuis_historico_integrante') %>%
                     dplyr::select(hogar_id, person_id, edad, anio, parentesco)

  df_trabajo <- large_table(connection = con,
                            schema = 'features',
                            the_table = 'cuis_trabajo')

  df_integrante <- dplyr::left_join(se_integrante, edad_integrante)

  df_salud <- dplyr::left_join(se_vivienda, df_integrante) %>%
              dplyr::left_join(df_trabajo) %>%
              dplyr::compute()

  print('Acceso a los Servicios de Salud')
  salud_vars <- c('atencion_medica', 'seguro_popular',
                  'am_imss', 'am_issste', 'am_issste_estatal',
                  'am_pemex', 'am_imss_prospera',
                  'am_otra', 'inscrito_prestacion_lab',
                  'inscrito_jubilacion', 'inscrito_familiar',
                  'inscrito_muerte_aseg', 'inscrito_estudiante',
                  'inscrito_contratacion', 'inscrito_familiar_otro',
                  'sm_lab', 'sm_cv', 'salud_dir',
                  'jefe_sm', 'cony_sm', 'hijo_sm',
                  'acceso_jefe_sm', 'acceso_cony_sm',
                  'acceso_hijo_sm', 'acceso_otros_sm',
                  'asalud', 'ic_asalud')

  print('Acceso a los Servicios de Salud')
  carencia_salud <- carencias_chunk(df = df_salud,
                                            chunkname = 'carencia_salud',
                                            vars = salud_vars,
                                            ids = c('person_id', 'hogar_id')) %>%
                            dplyr::compute()

  copy_to(con, carencia_salud,
          dbplyr::in_schema("features",'cuis_carencia_salud'),
          temporary = FALSE, overwrite = TRUE)
  dbDisconnect(con)

 # Carencia por Acceso a la Seguridad Social
  con <- prev_connect()


  df_ingresos <- large_table(connection = con,
                               schema = 'clean',
                               the_table = 'cuis_historico_se_integrante') %>%
  dplyr::select(one_of(selected_ingresos))

 print('Acceso a la Seguridad Social')
 ingresos_vars <- c('pam', 'ingreso_pens')
 ingresos_cuis <- carencias_chunk(df = df_ingresos,
                                  chunkname = 'ingresos_cuis',
                                  vars = ingresos_vars,
                                  ids = c('person_id', 'hogar_id'),
                                  return.all = TRUE) %>%
                  dplyr::compute()


 copy_to(con, ingresos_cuis,
          dbplyr::in_schema("features",'cuis_ingresos'),
          temporary = FALSE, overwrite = TRUE)
  dbDisconnect(con)

 con <- prev_connect()
 seguridad_social_vars <- c('sm_lab', 'jubilado', 'sm_cv',
                            'afore_cv', 'ss_dir',
                            'jefe_ss', 'cony_ss', 'hijo_ss',
                            'acceso_jefe_ss', 'acceso_cony_ss',
                            'acceso_hijo_ss', 'acceso_otros_ss',
                            'seguridad_social', 'ic_seguridad_social')

 carencia_seguridad_social_ind <- carencias_chunk(df = ingresos_cuis,
                                              chunkname = 'carencia_seguridad_social',
                                              vars = seguridad_social_vars,
                                              ids = c('hogar_id', 'person_id')) %>%
                              dplyr::compute()

 carencia_seguridad_social <- carencia_seguridad_social_ind %>%
                              dplyr::group_by(hogar_id) %>%
                              dplyr::summarise_at(seguridad_social_vars,
                                                  funs(max(., na.rm = TRUE))) %>%
                              dplyr::compute()

 print('Joining all tables')
 cuis_carencias <- carencia_educacion %>%
                   dplyr::left_join(carencia_alimentaria) %>%
                   dplyr::left_join(carencia_vivienda) %>%
                   dplyr::left_join(carencia_servicios) %>%
                   dplyr::left_join(dplyr::select(carencia_salud, -sm_lab, -sm_cv)) %>%
                   dplyr::left_join(carencia_seguridad_social)

 print('Copy to features.cuis_carencias')
 copy_to(con, cuis_carencias,
          dbplyr::in_schema("features",'cuis_carencias'),
          temporary = FALSE, overwrite = TRUE)
  dbDisconnect(con)

}
