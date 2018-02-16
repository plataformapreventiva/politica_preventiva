#encoding
library(tidyverse)
library(RPostgreSQL)
source('../utils/kringing_imputation.R')

###############################################################
######### MUNICIPIOS ###########
###############################################################


#### GET AMENAZAS - NATURALES
#r_alim = as_tibble(dbGetQuery(con, "SELECT * FROM semantic.r_alimentario_municipios;"))
nom_muns = as_tibble(dbGetQuery(con, "SELECT cve_muni, nom_mun FROM geoms.municipios;"))
eco = as_tibble(dbGetQuery(con, "SELECT cve_muni, capacidades_economia_complejidad_i,
                             capacidades_economia_productividad_i FROM clean.capacidades_economia_municipios;"))
inffi = as_tibble(dbGetQuery(con, "SELECT cve_muni, capacidades_infraestructurafisica_carreteras_i, 
                            capacidades_infraestructurafisica_agua_i, capacidades_infraestructurafisica_drenaje_i, 
                            capacidades_infraestructurafisica_refri_i 
                            FROM clean.capacidades_infraestructurafisica_municipios;"))
salu = as_tibble(dbGetQuery(con, "SELECT cve_muni, capacidades_salud_hospitales_i, 
                            capacidades_salud_medicos_i FROM clean.capacidades_salud_municipios;"))
desi = as_tibble(dbGetQuery(con, "SELECT cve_muni, vulnerabilidades_desigualdad_irs_i, 
                             vulnerabilidades_desigualdad_gini_i FROM clean.vulnerabilidades_desigualdad_municipios;"))
pobr = as_tibble(dbGetQuery(con, "SELECT cve_muni, vulnerabilidades_pobreza_educacion_i, 
                              vulnerabilidades_pobreza_alimentacion_i FROM clean.vulnerabilidades_pobreza_municipios;"))
pob = as_tibble(dbGetQuery(con, "SELECT cve_muni, vulnerabilidades_poblacion_fetal_i 
                           FROM clean.vulnerabilidades_poblacion_municipios;"))
conev <- as_tibble(dbGetQuery(con, "SELECT cve_muni, plb_p_10, pobreza_e_p_10 FROM raw.coneval_municipios;"))
coneval <- conev %>%
  mutate(pobreza_i = normalize(pobreza_e_p_10, F),
         linea_bienestar_i = normalize(plb_p_10, F)) %>%
  dplyr::select(cve_muni, ends_with('_i'))
natur  = as_tibble(dbGetQuery(con, "SELECT cve_muni, a_n_gp_bajaste_i, a_n_gp_sequia2_i,
         a_n_gp_inundac_i FROM clean.amenazas_naturales_municipios;")) # falta altera temperatura
nat <- natur %>%
  mutate(a_n_gp_bajaste_i = as.numeric(a_n_gp_bajaste_i),
         a_n_gp_sequia2_i = as.numeric(a_n_gp_sequia2_i),
         a_n_gp_inundac_i = as.numeric(a_n_gp_inundac_i))
hum = as_tibble(dbGetQuery(con, "SELECT cve_muni, amenazas_humana_violencia_i, 
                             a_h_volatilidad_i, a_h_e_inpc_i FROM clean.amenazas_humanas_municipios;"))

  
fao <- nom_muns %>%
  merge(eco, by='cve_muni', all.x=T) %>%
  merge(inffi, by='cve_muni', all.x=T) %>%
  merge(salu, by='cve_muni', all.x=T) %>%
  merge(desi, by='cve_muni', all.x=T) %>%
  merge(pobr, by='cve_muni', all.x=T) %>%
  merge(pob, by='cve_muni', all.x=T) %>%
  merge(coneval, by='cve_muni', all.x=T) %>% 
  merge(nat, by='cve_muni', all.x=T) %>%
  merge(hum, by='cve_muni', all.x=T) %>%
  rename(inpc_i = a_h_e_inpc_i,
         complejidad_i = capacidades_economia_complejidad_i,
         productividad_i = capacidades_economia_productividad_i,
         carreteras_i = capacidades_infraestructurafisica_carreteras_i,
         agua_i = capacidades_infraestructurafisica_agua_i,
         drenaje_i= capacidades_infraestructurafisica_drenaje_i,
         refri_i = capacidades_infraestructurafisica_refri_i, 
         hospitales_i = capacidades_salud_hospitales_i,
         medicos_i = capacidades_salud_medicos_i,
         irs_i = vulnerabilidades_desigualdad_irs_i, 
         gini_i = vulnerabilidades_desigualdad_gini_i,
         educacion_i = vulnerabilidades_pobreza_educacion_i,
         alimentacion_i = vulnerabilidades_pobreza_alimentacion_i,
         fetal_i = vulnerabilidades_poblacion_fetal_i, 
         temperaturas_i = a_n_gp_bajaste_i,
         inundaciones_i = a_n_gp_inundac_i,
         sequia_i = a_n_gp_sequia2_i,
         violencia_i = amenazas_humana_violencia_i) %>%
  mutate(
    disponibilidad_local_i = (remove_na(complejidad_i) * remove_na(productividad_i))^(.5),
    disponibilidad_comercial_i = (remove_na(carreteras_i) * remove_na(inpc_i))^(.5),
    disponibilidad_i = (disponibilidad_local_i * disponibilidad_comercial_i)^(.5),
    acceso_economico_i = (remove_na(linea_bienestar_i) *remove_na(gini_i))^(.5),
    acceso_autoconsumo_i = (remove_na(alimentacion_i)),
    acceso_i = (acceso_economico_i * acceso_autoconsumo_i)^(1/2),
    utilizacion_educativo_i= remove_na(educacion_i),
    utilizacion_salud_i = (remove_na(fetal_i) * remove_na(medicos_i) * remove_na(hospitales_i))^(1/3),
    utilizacion_vivienda_i = (remove_na(agua_i) * remove_na(drenaje_i) * remove_na(refri_i))^(1/3),
    utilizacion_i = (utilizacion_vivienda_i * utilizacion_salud_i * 
                       utilizacion_educativo_i)^(1/3),
    estabilidad_violencia_i = remove_na(violencia_i),
    estabilidad_natural_i = (remove_na(temperaturas_i) + remove_na(inundaciones_i) + remove_na(sequia_i))*(1/3),
    estabilidad_i = (.4*remove_na(estabilidad_violencia_i) + .6*remove_na(estabilidad_natural_i)),
    riesgo_alimentario_i = (disponibilidad_i * acceso_i * utilizacion_i * estabilidad_i)^(1/4),
    disponibilidad_r = rank(-disponibilidad_i),
    acceso_r = rank(-acceso_i),
    utilizacion_r = rank(-utilizacion_i),
    estabilidad_r = rank(-estabilidad_i),
    riesgo_alimentario_rn = rank(-riesgo_alimentario_i),
    disponibilidad_r_i = normalize(disponibilidad_r),
    acceso_r_i = normalize(acceso_r),
    utilizacion_r_i = normalize(utilizacion_r),
    estabilidad_r_i = normalize(estabilidad_r),
    riesgo_alimentario_r_i =  normalize(riesgo_alimentario_rn)
  ) %>% gather(dimension, dim_valor, disponibilidad_r_i:utilizacion_r_i) %>%
  group_by(cve_muni) %>% mutate(max_dim = factor(which.max(dim_valor), levels=c('1', '2', '3'))) %>%
  ungroup() %>%
  spread(dimension, dim_valor) %>%
  rowwise() %>%
  mutate(max_sdim=ifelse(max_dim == "1", 
                    max(disponibilidad_local_i, disponibilidad_local_i),
                    ifelse(max_dim == "2", 
                           max(acceso_economico_i, acceso_autoconsumo_i),
                           max(utilizacion_educativo_i, utilizacion_salud_i, utilizacion_vivienda_i))
                    )
         ) %>%
  gather(sub_dims, subvalor, disponibilidad_local_i, disponibilidad_comercial_i, 
         acceso_economico_i, acceso_autoconsumo_i, utilizacion_educativo_i, 
         utilizacion_salud_i, utilizacion_vivienda_i) %>%
  group_by(cve_muni) %>% 
  mutate(max_sdim = factor(which(max_sdim == subvalor), levels=c('1', '2', '3', '4', '5', '6', '7'))) %>%
  spread(sub_dims, subvalor) %>%
  mutate(max_dim = forcats::fct_recode(max_dim, 
                                       'Disponibilidad' = '1', 
                                       'Acceso' = '2',
                                       'Utilización' = '3'),
         max_sdim= forcats::fct_recode(max_sdim,
                                       'disponibilidad_local_i' = '1',
                                       'disponibilidad_comercial_i' = '2',
                                       'acceso_economico_i' = '3',
                                       'acceso_autoconsumo_i' = '4',
                                       'utilizacion_educativo_i' = '5',
                                       'utilizacion_salud_i' = '6',
                                       'utilizacion_vivienda_i' = '7'),
         cve_ent = substr(cve_muni, 0, 2)) %>%
  group_by(cve_ent) %>%
  mutate(riesgo_alimentario_re = rank(riesgo_alimentario_rn),
         num_muns = n())

dbWriteTable(con, c("clean",'riesgo_municipios'), fao, row.names=FALSE)



###############################################################
######### ESTADOS ###########
###############################################################


#### GET AMENAZAS - NATURALES
#r_alim = as_tibble(dbGetQuery(con, "SELECT * FROM semantic.r_alimentario_municipios;"))
nom_edos = as_tibble(dbGetQuery(con, "SELECT cve_ent, nom_ent FROM geoms.estados;"))
eco = as_tibble(dbGetQuery(con, "SELECT cve_ent, capacidades_economia_complejidad_i,
                           capacidades_economia_productividad_i FROM clean.capacidades_economia_estados;"))
inffi = as_tibble(dbGetQuery(con, "SELECT cve_ent, capacidades_infraestructurafisica_carreteras_i, 
                             capacidades_infraestructurafisica_agua_i, capacidades_infraestructurafisica_drenaje_i, 
                             capacidades_infraestructurafisica_refri_i 
                             FROM clean.capacidades_infraestructurafisica_estados;"))
salu = as_tibble(dbGetQuery(con, "SELECT cve_ent, capacidades_salud_hospitales_i, 
                            capacidades_salud_medicos_i FROM clean.capacidades_salud_estados;"))
desi = as_tibble(dbGetQuery(con, "SELECT cve_ent, vulnerabilidades_desigualdad_irs_i, 
                            vulnerabilidades_desigualdad_gini_i FROM clean.vulnerabilidades_desigualdad_estados;"))
pobr = as_tibble(dbGetQuery(con, "SELECT cve_ent, vulnerabilidades_pobreza_educacion_i, 
                            vulnerabilidades_pobreza_alimentacion_i FROM clean.vulnerabilidades_pobreza_estados;"))
pob = as_tibble(dbGetQuery(con, "SELECT cve_ent, vulnerabilidades_poblacion_fetal_i 
                           FROM clean.vulnerabilidades_poblacion_estados;"))
conev <- as_tibble(dbGetQuery(con, "SELECT cve_ent, plb_p_10, pobreza_e_p_10 FROM raw.coneval_estados;"))
coneval <- conev %>%
  mutate(pobreza_i = normalize(pobreza_e_p_10, F),
         linea_bienestar_i = normalize(plb_p_10, F)) %>%
  dplyr::select(cve_ent, ends_with('_i'))
natur  = as_tibble(dbGetQuery(con, "SELECT cve_ent, a_n_gp_bajaste_i, a_n_gp_sequia2_i,
                              a_n_gp_inundac_i FROM clean.amenazas_naturales_estados;")) # falta altera temperatura
nat <- natur %>%
  mutate(a_n_gp_bajaste_i = as.numeric(a_n_gp_bajaste_i),
         a_n_gp_sequia2_i = as.numeric(a_n_gp_sequia2_i),
         a_n_gp_inundac_i = as.numeric(a_n_gp_inundac_i))
hum = as_tibble(dbGetQuery(con, "SELECT cve_ent, amenazas_humana_violencia_i, 
                           a_h_volatilidad_i, a_h_e_inpc_i FROM clean.amenazas_humanas_estados;"))


fao <- nom_edos %>%
  merge(eco, by='cve_ent', all.x=T) %>%
  merge(inffi, by='cve_ent', all.x=T) %>%
  merge(salu, by='cve_ent', all.x=T) %>%
  merge(desi, by='cve_ent', all.x=T) %>%
  merge(pobr, by='cve_ent', all.x=T) %>%
  merge(pob, by='cve_ent', all.x=T) %>%
  merge(coneval, by='cve_ent', all.x=T) %>% 
  merge(nat, by='cve_ent', all.x=T) %>%
  merge(hum, by='cve_ent', all.x=T) %>%
  rename(inpc_i = a_h_e_inpc_i,
         complejidad_i = capacidades_economia_complejidad_i,
         productividad_i = capacidades_economia_productividad_i,
         carreteras_i = capacidades_infraestructurafisica_carreteras_i,
         agua_i = capacidades_infraestructurafisica_agua_i,
         drenaje_i= capacidades_infraestructurafisica_drenaje_i,
         refri_i = capacidades_infraestructurafisica_refri_i, 
         hospitales_i = capacidades_salud_hospitales_i,
         medicos_i = capacidades_salud_medicos_i,
         irs_i = vulnerabilidades_desigualdad_irs_i, 
         gini_i = vulnerabilidades_desigualdad_gini_i,
         educacion_i = vulnerabilidades_pobreza_educacion_i,
         alimentacion_i = vulnerabilidades_pobreza_alimentacion_i,
         fetal_i = vulnerabilidades_poblacion_fetal_i, 
         temperaturas_i = a_n_gp_bajaste_i,
         inundaciones_i = a_n_gp_inundac_i,
         sequia_i = a_n_gp_sequia2_i,
         violencia_i = amenazas_humana_violencia_i) %>%
  mutate(
    disponibilidad_local_i = (remove_na(complejidad_i) * remove_na(productividad_i))^(.5),
    disponibilidad_comercial_i = (remove_na(carreteras_i) * remove_na(inpc_i))^(.5),
    disponibilidad_i = (disponibilidad_local_i * disponibilidad_comercial_i)^(.5),
    acceso_economico_i = (remove_na(linea_bienestar_i) *remove_na(gini_i))^(.5),
    acceso_autoconsumo_i = (remove_na(alimentacion_i)),
    acceso_i = (acceso_economico_i * acceso_autoconsumo_i)^(1/2),
    utilizacion_educativo_i= remove_na(educacion_i),
    utilizacion_salud_i = (remove_na(fetal_i) * remove_na(medicos_i) * remove_na(hospitales_i))^(1/3),
    utilizacion_vivienda_i = (remove_na(agua_i) * remove_na(drenaje_i) * remove_na(refri_i))^(1/3),
    utilizacion_i = (utilizacion_vivienda_i * utilizacion_salud_i * 
                       utilizacion_educativo_i)^(1/3),
    estabilidad_violencia_i = remove_na(violencia_i),
    estabilidad_natural_i = (remove_na(temperaturas_i) + remove_na(inundaciones_i) + remove_na(sequia_i))*(1/3),
    estabilidad_i = (.4*remove_na(estabilidad_violencia_i) + .6*remove_na(estabilidad_natural_i)),
    riesgo_alimentario_i = (disponibilidad_i * acceso_i * utilizacion_i * estabilidad_i)^(1/4),
    disponibilidad_r = rank(-disponibilidad_i),
    acceso_r = rank(-acceso_i),
    utilizacion_r = rank(-utilizacion_i),
    estabilidad_r = rank(-estabilidad_i),
    riesgo_alimentario_rn = rank(-riesgo_alimentario_i),
    disponibilidad_r_i = normalize(disponibilidad_r),
    acceso_r_i = normalize(acceso_r),
    utilizacion_r_i = normalize(utilizacion_r),
    estabilidad_r_i = normalize(estabilidad_r),
    riesgo_alimentario_r_i =  normalize(riesgo_alimentario_rn)
  ) %>% gather(dimension, dim_valor, disponibilidad_r_i:utilizacion_r_i) %>%
  group_by(cve_ent) %>% mutate(max_dim = factor(which.max(dim_valor), levels=c('1', '2', '3'))) %>%
  ungroup() %>%
  spread(dimension, dim_valor) %>%
  rowwise() %>%
  mutate(max_sdim=ifelse(max_dim == "1", 
                         max(disponibilidad_local_i, disponibilidad_local_i),
                         ifelse(max_dim == "2", 
                                max(acceso_economico_i, acceso_autoconsumo_i),
                                max(utilizacion_educativo_i, utilizacion_salud_i, utilizacion_vivienda_i))
  )
  ) %>%
  gather(sub_dims, subvalor, disponibilidad_local_i, disponibilidad_comercial_i, 
         acceso_economico_i, acceso_autoconsumo_i, utilizacion_educativo_i, 
         utilizacion_salud_i, utilizacion_vivienda_i) %>%
  group_by(cve_ent) %>% 
  mutate(max_sdim = factor(which(max_sdim == subvalor), levels=c('1', '2', '3', '4', '5', '6', '7'))) %>%
  ungroup() %>%
  spread(sub_dims, subvalor) %>%
  mutate(max_dim = forcats::fct_recode(max_dim, 
                                       'Disponibilidad' = '1', 
                                       'Acceso' = '2',
                                       'Utilización' = '3'),
         max_sdim= forcats::fct_recode(max_sdim,
                                       'disponibilidad_local_i' = '1',
                                       'disponibilidad_comercial_i' = '2',
                                       'acceso_economico_i' = '3',
                                       'acceso_autoconsumo_i' = '4',
                                       'utilizacion_educativo_i' = '5',
                                       'utilizacion_salud_i' = '6',
                                       'utilizacion_vivienda_i' = '7'))


dbWriteTable(con, c("clean",'r_alimentario_estados'), fao, row.names=FALSE)
