#!/usr/bin/env Rscript
library(rlang)
library(tidyverse)

remove_char <- '&'
int_columns <- c('id_trabajo','trapais','subor','indep','personal',
                 'pago','contrato','tipocontr','pres_1','pres_2',
                 'pres_3','pres_4','pres_5','pres_6','pres_7','pres_8',
                 'pres_9','pres_10','pres_11','pres_12','pres_13',
                 'pres_14','pres_15','pres_16','pres_17','pres_18',
                 'pres_19','pres_20','pres_21','pres_22','pres_23',
                 'pres_24','pres_25','pres_26','htrab','sinco','scian',
                 'clas_emp','tam_emp','no_ing','tiene_suel','tipoact',
                 'socios','soc_nr1','soc_nr2','soc_resp','otra_act',
                 'tipoact2','tipoact3','tipoact4','lugar','conf_pers',
                 'reg_not','reg_cont','com_fis')
text_columns <- c()
replace_na_cols <- c('pres_7','pres_14')

make_clean <- function(pipeline_task, con){
  df <- tbl(con, dbplyr::in_schema('raw', pipeline_task))
  df %>% mutate_if(is.character, funs(ifelse(. == '&', NA, .)), ) %>%
    mutate_if(is.character, funs(trimws(.))) %>%
    mutate_if(is.character, funs(ifelse(. == '', NA, .)), ) %>%
    mutate_at(int_columns, funs(as.integer(.))) %>%
    mutate_at(replace_na_cols, funs(coalesce(., 2))) %>%
    mutate(hogar_id = paste(folioviv, foliohog, sep='-')) %>%
    mutate(person_id = paste(folioviv, foliohog, numren, sep='-')) %>%
    mutate(fecha_creacion = substr(data_date, 1, 4))
}
