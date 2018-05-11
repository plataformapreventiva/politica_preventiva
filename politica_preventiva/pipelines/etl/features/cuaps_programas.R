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
                         collect()


varnames_componentes <- c('indic', 'tipo_apoyo', 'tipo_pob_apo', 'apoyo_gen_padron')
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

cuaps_criterios <- tbl(predictivadb,  sql('select * from clean.cuaps_criterios')) %>%
                         collect() %>%
                         mutate(grupos_vulnerables = ifelse(chr_descripcion == 'C05', csc_configuracion_foc, NA),
                                atiende_pobreza = ifelse(chr_descripcion == 'C04', csc_configuracion_foc, NA),


varnames_criterios <- c()
plotnames_criterios <- c('s06_contribuciones', 's09_tipo_apoyos', 's09_tipo_poblaciones', 's10_padrones')

names_criterios <- map_df(1:length(varnames_criterios), function(x) get_varnames(data = cuaps_criterios,
                                                                              prefix = varnames_criterios[x],
                                                                              plot_title = plotnames_criterios[x]))


criterios_tidy <- map_df(1:nrow(names_criterios), function(x) tidy_count(cuaps_criterios,
                                                     names_criterios$varname[x],
                                                     names_criterios$plotname[x]))


