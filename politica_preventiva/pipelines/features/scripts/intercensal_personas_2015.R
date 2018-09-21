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
  
  #Población por municipio
  pob_mun <- intercensal %>% select(cve_muni, factor) %>%
    group_by(cve_muni) %>% 
    summarise(factor=as.numeric(sum(factor)))
  
  intercensal_aux <- intercensal %>% select(cve_muni,mayores65,menores5,
                                           adultos,menores,indigenas,lengua_indigena,
                                           afrodescendientes,mujeres,hombres) %>%
    left_join(pob_mun,by='cve_muni') %>% group_by(cve_muni) %>% 
    summarise_all(sum) %>% replace(is.na(.), 0) %>% 
    mutate(p_mayores65=100*mayores65/factor, p_menores5=100*menores5/factor, 
             p_adultos=100*adultos/factor, p_menores=100*menores/factor, 
             p_indigenas=100*indigenas/factor, p_lengua_indigena=100*lengua_indigena/factor, 
             p_afrodescendientes=100*afrodescendientes/factor,
             p_mujeres=100*mujeres/factor,p_hombres=100*hombres/factor) %>% 
    select(cve_muni,p_mayores65,p_menores5,p_adultos,p_menores,p_indigenas,
           p_lengua_indigena,p_afrodescendientes,p_mujeres,p_hombres)
  
  #MIGRACION INTERNA Y EXTERNA RECIENTE
  migracion_reciente <- intercensal %>% 
    mutate(migracion_interna_reciente = as.double(migrantes_internos_recientes)/poblacion_mun) %>%
    mutate(migracion_externa_reciente = as.double(migrantes)/poblacion_mun) %>%
    select(cve_muni,migracion_interna_reciente,migracion_externa_reciente) %>%
    group_by(cve_muni) %>% 
    summarise_all(mean,na.rm = TRUE)
  
  #POBLACION FLOTANTE
  
  #Población flotante por municipio debido a la escuela
  poblacion_flotante_esc <- intercensal %>% 
    mutate(pob_flot_esc = ifelse(cve_muni_esc == cve_muni, 0, 1)) %>%
    group_by(cve_muni) %>% summarise(pob_flot_esc = sum(pob_flot_esc,na.rm = TRUE)) %>% 
    left_join(pob_mun, by='cve_muni') %>%
    mutate(tasa_flot_esc = pob_flot_esc/factor) %>%
    select(cve_muni,tasa_flot_esc)
  
  #Población flotante por municipio debido al trabajo
  poblacion_flotante_trab <- intercensal %>% 
    mutate(pob_flot_trab = ifelse(cve_muni_trab == cve_muni, 0, 1)) %>% 
    group_by(cve_muni) %>% summarise(pob_flot_trab = sum(pob_flot_trab,na.rm = TRUE)) %>% 
    left_join(pob_mun, by='cve_muni') %>%
    mutate(tasa_flot_trab = pob_flot_trab/factor) %>%
    select(cve_muni,tasa_flot_trab)
  
  # BRECHA TIEMPO DE CUIDADOS
  horas <- intercensal %>% 
    select(cve_muni,sexo,acti_sin_pago1,acti_sin_pago2,
           acti_sin_pago3,acti_sin_pago4,acti_sin_pago5,acti_sin_pago6,
           acti_sin_pago7,acti_sin_pago8) %>% replace(is.na(.), 0) %>%
    mutate(total_horas = (acti_sin_pago1 + acti_sin_pago2 +
                            acti_sin_pago3 + acti_sin_pago4 + acti_sin_pago5 +
                            acti_sin_pago6 + acti_sin_pago7 + acti_sin_pago8)) %>%
    group_by(cve_muni,sexo) %>% summarise_all(mean,na.rm = TRUE)
  
  hombres <- horas %>% 
    filter(sexo == 1)  %>% mutate(total_horas_h=total_horas) %>% select(-c(sexo,total_horas))
  mujeres <- horas %>% 
    filter(sexo == 3) %>% mutate(total_horas_m=total_horas) %>% select(-c(sexo,total_horas))
  
  brecha_horas_cuidados <- left_join(hombres, mujeres, by = 'cve_muni') %>% 
    mutate(brecha_horas_cuidados = total_horas_m-total_horas_h) %>% 
    select(cve_muni, brecha_horas_cuidados)
  
# HOMBRES Y MUJERES EN EL MERCADO LABORAL
  ocupadas <- intercensal %>% 
    filter(sexo == 3) %>% mutate(edad_trabajar = ifelse(edad > 13, 1, 0),
           ocupadas = ifelse((conact == 10 | conact == 11 | conact ==12 |
                               conact ==13 | conact == 14 | conact == 15| 
                               conact ==16), 1,0)) %>%
    mutate(edad_trabajar_f = edad_trabajar*as.numeric(factor),
           ocupadas_f = ocupadas*as.numeric(factor)) %>% 
    select(cve_muni,ocupadas_f,edad_trabajar_f) %>%
    replace(is.na(.), 0) %>% group_by(cve_muni)  %>% summarise_all(sum) %>%
    mutate(proporcion_mujeres_ocupadas = ocupadas_f/edad_trabajar_f) %>%
    select(cve_muni, proporcion_mujeres_ocupadas)
  
  ocupados <- intercensal %>% 
    filter(sexo == 1) %>% 
    mutate(edad_trabajar = ifelse(edad > 13, 1, 0),
           ocupados = ifelse(conact == 10 | conact == 11 | conact ==12 |
                               conact ==13 |conact == 14 | conact == 15 |
                               conact ==16, 1,0)) %>% 
    mutate(edad_trabajar_f = edad_trabajar*as.numeric(factor),
           ocupados_f = ocupados*as.numeric(factor)) %>% 
    select(cve_muni,ocupados_f,edad_trabajar_f) %>% 
    replace(is.na(.), 0) %>% group_by(cve_muni)  %>% summarise_all(sum) %>%
    mutate(proporcion_hombres_ocupados = ocupados_f/edad_trabajar_f) %>%
    select(cve_muni, proporcion_hombres_ocupados)
  
  # JOIN 
  intercensal_personas_2015 <- left_join(poblacion_flotante_esc,poblacion_flotante_trab, by= 'cve_muni') %>%
    left_join(migracion_reciente, by='cve_muni') %>%
    left_join(ocupados, by='cve_muni') %>%
    left_join(ocupadas, by='cve_muni') %>%
    left_join(brecha_horas_cuidados, by='cve_muni') %>%
    left_join(intercensal_aux, by='cve_muni') %>%
    dplyr::mutate(actualizacion_sedesol = lubridate::today())
  
  copy_to(con, intercensal_personas_2015,
          dbplyr::in_schema("features",'intercensal_personas_2015'),
          temporary = FALSE, overwrite = TRUE)
  dbDisconnect(con)
  
  print('Features written to: features.intercensal_personas_2015')
}
