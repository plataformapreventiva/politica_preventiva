#!/usr/bin/env Rscript
library(rlang)
library(tidyverse)

remove_char <- '&'
int_columns <- c('foliohog','huespedes','huesp_come','num_trab_d',
                 'trab_come','acc_alim1','acc_alim2','acc_alim3',
                 'acc_alim4','acc_alim5','acc_alim6','acc_alim7',
                 'acc_alim8','acc_alim9','acc_alim10','acc_alim11',
                 'acc_alim12','acc_alim13','acc_alim14','acc_alim15',
                 'acc_alim16','alim17_1','alim17_2','alim17_3','alim17_4',
                 'alim17_5','alim17_6','alim17_7','alim17_8','alim17_9',
                 'alim17_10','alim17_11','alim17_12','acc_alim18',
                 'telefono','celular','tv_paga','conex_inte','num_auto',
                 'num_bici','num_trici','num_carret','num_canoa','num_otro',
                 'num_ester','num_grab','num_radio','num_tva','num_tvd','num_dvd',
                 'num_video','num_licua','num_tosta','num_micro','num_refri',
                 'num_estuf','num_lavad','num_planc','num_maqui','num_venti',
                 'num_aspir','num_compu','num_impre','num_juego','esc_radio',
                 'er_aparato','er_celular','er_compu','er_aplicac','er_tv',
                 'er_otro','recib_tvd','tsalud1_h','tsalud1_m','tsalud1_c',
                 'tsalud2_h','tsalud2_m','habito_1','habito_2','habito_3',
                 'habito_4','habito_5','habito_6','consumo','nr_viv','tarjeta',
                 'pagotarjet','regalotar','regalodado','autocons','regalos',
                 'remunera','transferen','parto_g','embarazo_g','negcua',
                 'est_alim','est_trans','bene_licon','cond_licon','lts_licon',
                 'otros_lts','diconsa','frec_dicon','cond_dicon','pago_dicon',
                 'otro_pago','factor_hog')
text_columns <- c('anio_auto','anio_bici', 'anio_trici','anio_carret',
                  'anio_canoa','anio_otro','anio_ester','anio_grab',
                  'anio_radio','anio_tva','anio_tvd','anio_dvd','anio_video',
                  'anio_licua','anio_tosta','anio_micro','anio_refri',
                  'anio_estuf','anio_lavad','anio_planc','anio_maqui',
                  'anio_venti','anio_aspir','anio_compu','anio_impre','anio_juego')
replace_na_cols <- c('acc_alim1','acc_alim2','acc_alim3',
                     'acc_alim4','acc_alim5','acc_alim6','acc_alim7',
                     'acc_alim8','acc_alim9','acc_alim10','acc_alim11',
                     'acc_alim12','acc_alim13','acc_alim14','acc_alim15',
                     'acc_alim16','alim17_1','alim17_2','alim17_3','alim17_4',
                     'alim17_5','alim17_6','alim17_7','alim17_8','alim17_9',
                     'alim17_10','alim17_11','alim17_12','acc_alim18')

make_clean <- function(pipeline_task, con){
  df <- tbl(con, dbplyr::in_schema('raw', pipeline_task))
  df %>% mutate_if(is.character, funs(ifelse(. == '&', NA, .)), ) %>%
    mutate_at(int_columns, funs(as.integer(.))) %>%
    mutate_at(replace_na_cols, funs(coalesce(., 2))) %>%
    mutate_at(text_columns, funs(as.character(.)) ) %>%
    mutate(hogar_id = paste(folioviv, foliohog, sep='-')) %>%
    mutate(fecha_creacion = substr(data_date, 1, 4))
}
