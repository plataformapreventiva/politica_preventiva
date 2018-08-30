#!/usr/bin/env Rscript

int_columns <- c('c_instsal_a', 'c_instsal_b',
                 'c_afilsal_a', 'c_afilsal_b',
                 'c_lengua_ind', 'habl_esp',
                 'indigena', 'leer_escr',
                 'c_ult_nivel', 'c_ult_gra',
                 'asis_esc', 'c_aban_escu',
                 'c_con_tra', 'c_ver_con_trab',
                 'c_pos_ocup', 'c_peri_tra',
                 'c_mot_notr', 'trab_subor',
                 'trab_indep', 'trab_presta_a',
                 'trab_presta_b', 'trab_presta_c',
                 'trab_presta_d', 'trab_presta_e',
                 'trab_presta_f', 'trab_presta_g',
                 'trab_presta_h', 'trab_no_re',
                 'c_periodo', 'seg_volunt_a',
                 'seg_volunt_b', 'seg_volunt_c',
                 'seg_volunt_d', 'seg_volunt_e',
                 'seg_volunt_f', 'seg_volunt_g',
                 'jubilado', 'jubilado_1',
                 'jubilado_2', 'inapam',
                 'am_a', 'am_b', 'am_c', 'am_d',
                 'am_e', 'otr_ing_a', 'otr_ing_b',
                 'otr_ing_c', 'otr_ing_d',
                 'otr_ing_e', 'otr_ing_f', 'otr_ing_g',
                 'tiene_disca', 'disca_ori', 'disca_gra',
                 'tiene_discb', 'discb_ori', 'discb_gra',
                 'tiene_discc', 'discc_ori', 'discc_gra',
                 'tiene_discd', 'discd_ori', 'discd_gra',
                 'tiene_disce', 'disce_ori', 'disce_gra',
                 'tiene_discf', 'discf_ori', 'discf_gra',
                 'enf_art', 'enf_can', 'enf_cir',
                 'enf_ren', 'enf_dia', 'enf_cor',
                 'enf_pul', 'enf_vih', 'enf_def',
                 'enf_hip', 'enf_obe',
                 'p_agri', 'p_manu', 'p_come', 'p_tran',
                 'p_prof', 'p_educ', 'p_sald', 'p_recr',
                 'p_aloj', 'p_comu', 'p_otro',
                 'c_raz_no_trab')

float_columns <- c('monto', 'otr_ing_a_2',
                   'otr_ing_b_2', 'otr_ing_c_2',
                   'otr_ing_c_2', 'otr_ing_d_2',
                   'otr_ing_e_2', 'otr_ing_f_2')

text_columns <- c('llave_hogar_h', 'fch_creacion',
                  'usr_creacion', 'csc_hogar',
                  'actualizacion_sedesol', 'data_date')


replace_na <- c('')

# The ETL pipeline will call this function to run UpdateCleanDB task

make_clean <- function(pipeline_task, con){
  df <- tbl(con, dbplyr::in_schema('raw', pipeline_task))
  df %>%
      mutate_at(vars(one_of(int_columns)), as.integer) %>%
      mutate_at(vars(one_of(float_columns)), as.double) %>%
      mutate_at(vars(one_of(replace_na)), function(x) if_else(is.na(x), 2, x)) %>%
      mutate(hogar_id = llave_hogar_h) %>%
      mutate(person_id = paste(llave_hogar_h, c_integrante, sep='-'))
}
