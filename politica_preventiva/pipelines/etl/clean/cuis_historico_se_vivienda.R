#!/usr/bin/env Rscript
library(rlang)
library(tidyverse)

int_columns <- c('c_salud_hoga', 'c_salud_hogb',
                 'ut_cuida1', 'ut_cuida2',
                 'ut_volun1', 'ut_volun2',
                 'ut_repara1', 'ut_repara2',
                 'ut_limpia1', 'ut_limpia2',
                 'ut_acarrea1', 'ut_acarrea2',
                 'con_remesa', 'com_dia',
                 'com_dia_nsnr', 'cereal',
                 'verduras', 'frutas', 'leguminosas',
                 'carne_huevo', 'lacteos', 'grasas',
                 'seg_alim_1', 'seg_alim_2',
                 'seg_alim_3', 'seg_alim_4',
                 'seg_alim_5', 'seg_alim_a',
                 'seg_alim_b', 'seg_alim_c',
                 'seg_alim_d', 'seg_alim_e',
                 'seg_alim_f', 'seg_alim_g',
                 'desay_nin', 'desay_lugar',
                 'desay_razon', 'cuart',
                 'cua_dor', 'coc_duer',
                 'c_piso_viv', 'condi_piso',
                 'cuar_pis_t', 'c_tech_viv',
                 'condi_techo', 'c_muro_viv',
                 'condi_muro', 'c_escusado',
                 'uso_exc', 'c_agua_a',
                 'trat_agua_a', 'trat_agua_b',
                 'trat_agua_c', 'trat_agua_d',
                 'trat_agua_e', 'trat_agua_f',
                 'c_con_drena', 'c_basura',
                 'c_combus_cocin', 'fogon_chim',
                 'ts_refri', 'ts_lavadora',
                 'ts_vhs_dvd_br', 'ts_vehi',
                 'ts_telefon', 'ts_micro',
                 'ts_compu', 'ts_est_gas',
                 'ts_boiler', 'ts_internet',
                 'ts_celular', 'ts_television',
                 'ts_tv_digital', 'ts_tv_paga',
                 'ts_tinaco', 'ts_clima',
                 'c_luz_ele', 'c_sit_viv',
                 'escritura1', 'escritura2',
                 'esp_niveles', 'esp_construc',
                 'esp_local', 'tie_agri',
                 'prop_tierra1', 'prop_tierra2',
                 'c_maiz', 'c_frij', 'c_cere',
                 'c_frut', 'c_cana', 'c_jito', 'c_chil',
                 'c_limn', 'c_papa', 'c_cafe', 'c_cate',
                 'c_forr', 'c_otro', 'c_ning',
                 'cul_riego', 'cul_maquina', 'cul_anim',
                 'cul_ferorg', 'cul_ferquim',
                 'cul_plagui', 'uso_hid_tra',
                 'caballos', 'burros', 'bueyes',
                 'chivos', 'reses', 'gallinas',
                 'cerdos', 'conejos', 'proyecto',
                 'piso_prog', 'escusado_prog')

float_columns <- c('construc_med', 'local_med',
                   'gas_alim', 'gas_vest', 'gas_educ')

text_columns <- c('llave_hogar_h', 'actualizacion_sedesol',
                  'data_date', 'fch_creacion', 'usr_creacion',
                  'csc_hogar')

replace_na <- c('seg_alim_1', 'seg_alim_2',
                'seg_alim_3', 'seg_alim_4',
                'seg_alim_5', 'seg_alim_a',
                'seg_alim_b', 'seg_alim_c',
                'seg_alim_d', 'seg_alim_e',
                'seg_alim_f', 'seg_alim_g')

# The ETL pipeline will call this function to run UpdateCleanDB task

make_clean <- function(pipeline_task, con){
  df <- tbl(con, dbplyr::in_schema('raw', pipeline_task))
  df %>%
      mutate_at(vars(one_of(int_columns)), as.integer) %>%
      mutate_at(vars(one_of(float_columns)), as.double) %>%
      mutate_at(vars(one_of(replace_na)), funs(ifelse(is.na(.), 2, .))) %>%
      mutate(hogar_id = llave_hogar_h)
}
