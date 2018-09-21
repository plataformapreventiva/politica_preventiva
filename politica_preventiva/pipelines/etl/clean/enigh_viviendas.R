#!/usr/bin/env Rscript
library(rlang)
library(tidyverse)

remove_char <- '&'
int_columns <- c('tipo_viv', 'mat_pared', 'mat_techos', 'mat_pisos',
                 'antiguedad', 'antigua_ne', 'cocina', 'cocina_dor',
                 'num_cuarto', 'disp_agua', 'dotac_agua', 'excusado',
                 'uso_compar', 'sanit_agua', 'drenaje', 'disp_elect',
                 'combustible', 'estufa_chi', 'eli_basura', 'tenencia',
                 'hog_dueno1', 'hog_dueno2', 'escrituras', 'tipo_finan',
                 'renta','estim_pago', 'pago_viv', 'pago_mesp', 'tipo_adqui',
                 'viv_usada', 'lavadero', 'fregadero', 'regadera', 'tinaco_azo',
                 'cisterna', 'pileta', 'medidor_luz', 'bomba_agua','tanque_gas',
                 'aire_acond', 'calefacc', 'tot_resid', 'tot_hom', 'tot_muj',
                 'tot_hog', 'tam_loc', 'est_socio', 'est_dis', 'upm',
                 'factor_viv', 'num_focos', 'calentador')
text_columns <- c('num_dueno1', 'num_dueno2', 'ageb','ubica_geo')

make_clean <- function(pipeline_task, con){
  df <- tbl(con, dbplyr::in_schema('raw', pipeline_task))
  df %>% mutate_if(is.character, funs(ifelse(. == '&', NA, .)), ) %>%
    mutate_at( int_columns, funs(as.integer(.))) %>%
    mutate_at(text_columns, funs(as.character(.)) ) %>%
    mutate(fecha_creacion = substr(data_date, 1, 4))
}
