#!/usr/bin/env Rscript
library(rlang)
library(tidyverse)

text_columns <- c('ent_regis','mun_regis','ent_resid','mun_resid','tloc_resid','loc_resid',
                 'tloc_ocurr','loc_ocurr','causa_def','lista_mex',
                 'sexo','edad','dia_ocurr','mes_ocurr','anio_ocur','dia_regis','mes_regis','anio_regis',
                 'dia_nacim','mes_nacim','razon_m','dis_re_oax',
                 'anio_nacim','ocupacion','escolarida','edo_civil','presunto','ocurr_trab','lugar_ocur',
                 'necropsia','asist_medi','sitio_ocur','cond_cert','nacionalid','derechohab',
                 'embarazo','rel_emba','horas','minutos','capitulo','grupo','lista1','gr_lismex',
                 'vio_fami','area_ur','edad_agru','complicaro','dia_cert','mes_cert','anio_cert',
                 'maternas','lengua','cond_act','par_agre','ent_ocules','mun_ocules','loc_ocules')

query <- 'SELECT LPAD(ent_ocurr::text, 2, \'0\') || LPAD(mun_ocurr::text, 3, \'0\') as cve_muni, 
                  ent_regis,mun_regis,ent_resid,mun_resid,tloc_resid,loc_resid,
                  tloc_ocurr,loc_ocurr,causa_def,lista_mex,sexo,edad,dia_ocurr,
                  mes_ocurr,anio_ocur,dia_regis,mes_regis,anio_regis,dia_nacim,mes_nacim,
                  anio_nacim,ocupacion,escolarida,edo_civil,presunto,ocurr_trab,lugar_ocur,
                  necropsia,asist_medi,sitio_ocur,cond_cert,nacionalid,derechohab,
                  embarazo,rel_emba,horas,minutos,capitulo,grupo,lista1,gr_lismex,
                  vio_fami,area_ur,edad_agru,complicaro,dia_cert,mes_cert,anio_cert,
                  maternas,lengua,cond_act,par_agre,ent_ocules,mun_ocules,loc_ocules,
                  razon_m,dis_re_oax
          FROM raw.defunciones_generales'

make_clean <- function(pipeline_task, con){
  df <- tbl(con, sql(query)) %>%
    mutate_at(text_columns, funs(as.character(.)))
}
