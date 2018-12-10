#!/usr/bin/env Rscript
library(rlang)
library(tidyverse)

float_columns <- c('autonomia_gasto_burocratico','autonomia_inversion_publica','autonomia_financiera',
                   'autonomia_operativa','capacidad_inversion','capacidad_operativa',
                   'costo_burocratico','costo_operacion','dependencia_aportaciones',
                   'dependencia_participaciones','faism_per_capita','flexibilidad_financiera',
                   'impuestos_per_capita','ingresos_propios_per_capita','inversion_publica_per_capita',
                   'participaciones_per_capita','deuda_entre_ingresos_totales','deuda_entre_ingresos_disponible')
text_columns <- c('cve_muni','nom_ent','nom_mun')
int_columns <- c('anio')

make_clean <- function(pipeline_task, con){
  df <- tbl(con, dbplyr::in_schema('raw', pipeline_task)) %>%
    mutate_at(text_columns, funs(as.character(.))) %>%
    mutate_at(float_columns, funs(as.numeric(.))) %>%
    mutate_at(int_columns, funs(as.integer(.)))
}
