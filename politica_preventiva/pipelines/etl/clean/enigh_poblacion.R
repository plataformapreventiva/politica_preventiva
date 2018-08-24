#!/usr/bin/env Rscript
library(rlang)
library(tidyverse)

remove_char <- '&'
int_columns <- c('numren','parentesco','sexo','edad',
                 'madre_hog','padre_hog','disc1','disc2',
                 'disc3', 'disc4', 'disc5','disc6','disc7',
                 'causa1','causa2','causa3','causa4','causa5',
                 'causa6','causa7','hablaind','hablaesp','comprenind',
                 'etnia','alfabetism','asis_esc','nivel','grado',
                 'tipoesc','tiene_b','otorg_b','forma_b','tiene_c',
                 'otorg_c','forma_c','nivelaprob','gradoaprob',
                 'antec_esc','edo_conyug','pareja_hog','segsoc',
                 'ss_aa','ss_mm','redsoc_1','redsoc_2','redsoc_3',
                 'redsoc_4','redsoc_5','redsoc_6','hor_1','min_1',
                 'usotiempo1','hor_2','min_2','usotiempo2','hor_3',
                 'min_3','usotiempo3','hor_4','min_4','usotiempo4',
                 'hor_5','min_5','usotiempo5','hor_6','min_6',
                 'usotiempo6','hor_5','min_5','usotiempo5','hor_6',
                 'min_6','usotiempo6','hor_7','min_7','usotiempo7',
                 'hor_8','min_8','usotiempo8','segpop','atemed',
                 'inst_1','inst_2','inst_3','inst_4','inst_5',
                 'inst_6','inscr_1','inscr_2','inscr_3','inscr_4',
                 'inscr_5','inscr_6','inscr_7','inscr_8','prob_anio',
                 'prob_sal','aten_sal','servmed_1','servmed_2',
                 'servmed_3','servmed_4','servmed_5','servmed_6',
                 'servmed_7','servmed_8','servmed_9','servmed_10',
                 'servmed_11','hh_lug','mm_lug','hh_esp','mm_esp',
                 'pagoaten_1','pagoaten_2','pagoaten_3','pagoaten_4',
                 'pagoaten_5','pagoaten_6','pagoaten_7','noatenc_1',
                 'noatenc_2','noatenc_3','noatenc_4','noatenc_5',
                 'noatenc_6','noatenc_7','noatenc_8','noatenc_9',
                 'noatenc_10','noatenc_11','noatenc_12','noatenc_13',
                 'noatenc_14','noatenc_15','noatenc_16','norecib_1',
                 'norecib_2','norecib_3','norecib_4','norecib_5',
                 'norecib_6','norecib_7','norecib_8','norecib_9',
                 'norecib_10','norecib_11','razon_1','razon_2','razon_3',
                 'razon_4','razon_5','razon_6','razon_7','razon_8',
                 'razon_9','razon_10','razon_11','diabetes','pres_alta',
                 'peso','segvol_1','segvol_2','segvol_3','segvol_4',
                 'segvol_5','segvol_6','segvol_7','hijos_viv','hijos_mue',
                 'hijos_sob','trabajo_mp','motivo_aus','act_pnea1',
                 'act_pnea2','act_buscot','act_rento','act_pensio',
                 'act_quehac','act_estudi','act_discap','act_otra','num_trabaj')
text_columns <- c('madre_id', 'padre_id','lenguaind','residencia',
                  'conyuge_id','prob_mes')
replace_2_cols <- c('atemed','trabajo_mp')
replace_0_cols <- c('segvol_1','segvol_2',
                    'inst_1','inst_2','inst_3','inst_4','inst_5',
                    'inst_6','inscr_1','inscr_2','inscr_3','inscr_4',
                    'inscr_5','inscr_6','inscr_7','inscr_8')


make_clean <- function(pipeline_task, con){
  df <- tbl(con, dbplyr::in_schema('raw', pipeline_task))
  df %>% mutate_if(is.character, funs(ifelse(. == '&', NA, .)), ) %>%
    mutate_if(is.character, funs(trimws(.))) %>%
    mutate_if(is.character, funs(ifelse(. == '', NA, .)), ) %>%
    mutate_at( int_columns, funs(as.integer(.))) %>%
    mutate_at(replace_2_cols, funs(coalesce(., 2))) %>%
    mutate_at(replace_0_cols, funs(coalesce(., 0))) %>%
    mutate_at(text_columns, funs(as.character(.)) ) %>%
    mutate(hogar_id = paste(folioviv, foliohog, sep='-')) %>%
    mutate(person_id = paste(folioviv, foliohog, numren, sep='-')) %>%
    mutate(fecha_creacion = substr(data_date, 1, 4)) %>%
    mutate(anio = substr(data_date, 1, 4)) %>%
    mutate(anio = as.integer(anio))
}
