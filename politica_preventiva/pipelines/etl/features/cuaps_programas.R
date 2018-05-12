#!/usr/bin/env Rscript
library(DBI)
library(rlang)
library(tidyverse)

source('/home/monizamudio/workspace/income-distribution/scripts/connect.R')

uncounted_vars <- c('actualizacion_sedesol', 'data_date')

tidy_count <- function(data, count_var, plotname, uncounted = uncounted_vars){

  count_var_name <- sym(count_var)
  count_var_quo <- quo(!! count_var_name)

  data %>%
      dplyr::group_by_at(vars(one_of(uncounted))) %>%
      dplyr::count(!! count_var_quo) %>%
      dplyr::mutate(plot = plotname,
                    variable = count_var) %>%
      dplyr::rename(valor = !! count_var_quo,
                    count = n) %>%
      dplyr::ungroup() %>%
      dplyr::filter(!is.na(valor)) %>%
      dplyr::mutate(valor = as.character(valor))
}

get_varnames <- function(data, prefix, plot_title){
  data %>%
      names() %>%
      grep(prefix, ., value = T) %>%
      tibble(varname = ., plotname = plot_title)
}

get_categories_cuaps <- function(condition, categories){
ifelse(cuaps_criterios$chr_descripcion == condition & cuaps_criterios$csc_configuracion_foc %in% categories, cuaps_criterios$csc_configuracion_foc, NA)
}

########################################
# Programs data
########################################

cuaps_programas <- tbl(predictivadb, sql("select * from clean.cuaps_programas")) %>%
                      collect()

varnames_programas <- c('orden_gob', 'nom_entidad_federativa', 'der_social')
plotnames_programas <- c('s01_ordengob', 's02_estados', 's03_derechos')

names_programas <- map_df(1:length(varnames_programas), function(x) get_varnames(data = cuaps_programas,
                                                                              prefix = varnames_programas[x],
                                                                              plot_title = plotnames_programas[x]))


programas_tidy <- map_df(1:nrow(names_programas), function(x) tidy_count(cuaps_programas,
                                                     names_programas$varname[x],
                                                     names_programas$plotname[x]))

#######################################
# Apoyos data
#######################################

cuaps_componentes <- tbl(predictivadb,  sql('select * from clean.cuaps_componentes')) %>%
                         collect() %>%
                         mutate(c_indic_alimentaria = indic_a + indic_b,
                                c_indic_vivienda = indic_c + indic_d + indic_e + indic_f,
                                c_indic_servicios = indic_g + indic_h + indic_i + indic_j,
                                c_indic_salud = indic_k,
                                c_indic_educacion = indic_l + indic_m + indic_n,
                                c_indic_segsocial = indic_o + indic_p + indic_q,
                                c_indic_ingreso_lb = indic_r,
                                c_indic_ingreso_lbm = indic_s)


varnames_componentes <- c('c_indic', 'tipo_apoyo', 'tipo_pob_apo', 'apoyo_gen_padron')
plotnames_componentes <- c('s06_contribuciones', 's09_tipo_apoyos', 's09_tipo_poblaciones', 's10_padrones')

names_componentes <- map_df(1:length(varnames_componentes), function(x) get_varnames(data = cuaps_componentes,
                                                                              prefix = varnames_componentes[x],
                                                                              plot_title = plotnames_componentes[x]))


componentes_tidy <- map_df(1:nrow(names_componentes), function(x) tidy_count(cuaps_componentes,
                                                     names_componentes$varname[x],
                                                     names_componentes$plotname[x]))


#######################################################################################
# Criterios data
#######################################################################################
criterios_grupos_vulnerables <- c(36, 1754, 1755, 1756, 1757, 37, 38, 39, 40, 41, 42,
                                  167, 168, 169, 43, 170, 171, 172, 173, 174, 175, 176, 44)

criterios_key <- tibble(old_code = criterios_grupos_vulnerables) %>%
                    dplyr::mutate(new_code = c(rep('Adultos Mayores', 5),
                                               'Migrantes',
                                               'Embarazadas o en periodo de lactancia',
                                               'Jefas de familia',
                                               'Madres o padres solos',
                                               'Enfermos crónicos',
                                               rep('Población indígena', 4),
                                               rep('Personas con discapacidad', 8),
                                               'Otro grupo vulnerable'))

criterios_pobreza <- 31:35
criterios_carencias <- 1:6
criterios_territorio <- 130:133
criterios_zap <- 158:160
criterios_marginacion <- 134:145

cuaps_criterios <- tbl(predictivadb,  sql('select * from clean.cuaps_criterios')) %>%
                         collect() %>%
                         mutate(atiende_grupos_vulnerables_old = recode(get_categories_cuaps('C05', criterios_grupos_vulnerables),
                                                                    ),
                                atiende_pobreza = get_categories_cuaps('C04', criterios_pobreza),
                                atiende_carencias = get_categories_cuaps('C01', criterios_carencias),
                                tiene_criterio_territorial = as.numeric(chr_descripcion == 'C03'),
                                atiende_territorios = get_categories_cuaps('C03', criterios_territorio),
                                atiende_zap = get_categories_cuaps('C03', criterios_zap),
                                atiende_marginacion = get_categories_cuaps('C03', criterios_marginacion)) 
                         %>%
                         left_join(criterios_key, by.x = 'atiende_grupos_vulnerables_old', by.y = 'old_code') %>%
                         mutate(atiende_grupos_vulnerables = new_code)



varnames_criterios <- c('atiende_grupos_vulnerables', 'atiende_pobreza', 'atiende_carencias',
                        'tiene_criterio_territorial', 'atiende_territorios', 'atiende_zap'
                        'atiende_marginacion')

plotnames_criterios <- c('s04_grupos_vulnerables', 's05_pobreza', 's07_carencias', 's08_criterio_territorial',
                         's08_territorios', 's08_zap', 's08_marginacion')

names_criterios <- map_df(1:length(varnames_criterios), function(x) get_varnames(data = cuaps_criterios,
                                                                              prefix = varnames_criterios[x],
                                                                              plot_title = plotnames_criterios[x]))


criterios_tidy <- map_df(1:nrow(names_criterios), function(x) tidy_count(cuaps_criterios,
                                                     names_criterios$varname[x],
                                                     names_criterios$plotname[x]))


