###############################################################################
# Clean to Explorador Municipios y Estados 
###############################################################################

rm(list=ls())
suppressPackageStartupMessages({
  library(psych)
  library(jsonlite)
  library(ggplot2)
  library(tidyverse)
  library(lubridate)
  require(zoo)
  library(stringr)
  library('RPostgreSQL')
  source("../shameful/utils_shameful.R")
  source("./utilsAPI.R")
  library(clusterSim)
  library(foreign)
  library("rgdal")
  library("rgeos")
  library("dplyr")
  library(mice)
})


##################
## Create Connection to DB
##################
conf <- fromJSON("../conf/conf_profile.json")
pg = dbDriver("PostgreSQL")
con = dbConnect(pg, user=conf$PGUSER, password=conf$PGPASSWORD,
                host=conf$PGHOST, port=5432, dbname=conf$PGDATABASE)

##################
## Semantic municipios
##################
municipios = as_tibble(dbGetQuery(con, "select * from raw.semantic_municipios;"))
colnames(municipios) = dbSafeNames(colnames(municipios))
municipios<-round_df(municipios, digits=3)

municipios<-municipios %>% dplyr::select(cve_muni,nom_mun,cve_ent,pobreza_p_10,pobreza_m_p_10, pobreza_e_p_10, vul_car_p_10, vul_ing_p_10, 
                                         no_pobv_p_10, pobreza_n_10, pobreza_m_n_10, pobreza_e_n_10, vul_car_n_10, vul_ing_n_10, no_pobv_n_10,
                                         s052, p_0424, s057, s061, s065, s071, s072, s118, s174, s176, s216, s241, u009, e003, p_0196, s017, e016, p_0342,
                                         gini_90,gini_00, gini_10,irez_soc10,irez_soc00,irez_soc05,irez_soc15,pobtot_15,porc_pob_snservsal15,porc_pob_15_analfa15,
                                         porc_snaguaent15,porc_vivsndren15,porc_vivsnenergia15,porc_vivsnsan15,carencias_n_10,ic_ali_n_10,
                                         ic_segsoc_n_10,ic_sbv_n_10,ic_asalud_n_10,ic_cev_n_10,plb_n_10,ic_rezedu_n_10,carencias3_n_10,
                                         pobreza_n_10,pobreza_e_n_10,pobreza_m_n_10,no_pobv_n_10,vul_car_n_10,vul_ing_n_10,plb_p_10,
                                         carencias_p_10,ic_ali_p_10,ic_segsoc_p_10,ic_sbv_p_10,ic_asalud_p_10,ic_cev_p_10,ic_rezedu_p_10)


municipios <- municipios %>% dplyr::mutate_each(funs(as.numeric(.)),matches("^[s|p\\_|e|u][0-9]+", ignore.case=FALSE))

colnames(municipios) <- dbSafeNames_explorador(colnames(municipios),9)
dbGetQuery(con, "DROP TABLE semantic.semantic_municipios;")
dbWriteTable(con, c("semantic",'semantic_municipios'),municipios, row.names=FALSE)

clean_dic <- colnames(municipios)


#municipios_dic = as_tibble(dbGetQuery(con, "select * from raw.pub_diccionario_programas;"))
municipios_dic = as_tibble(dbGetQuery(con, "select * from raw.semantic_municipios_dic;"))
municipios_dic$id = dbSafeNames(municipios_dic$id)
municipios_dic$id <- dbSafeNames_explorador(municipios_dic$id,9)

municipios_dic <- municipios_dic %>% dplyr::filter(id %in% clean_dic)



write_csv(municipios_dic,"semantic_municipios_dic.csv")
municipios_dic<- read_csv("semantic_municipios_dic.csv")

# Menos Variables
#municipios_dic<- read_csv("mun_dic_temp_2.csv")
#municipios_dic$id <- dbSafeNames(municipios_dic$id)
#municipios_dic$id <- dbSafeNames_explorador(municipios_dic$id,n)

dbGetQuery(con, "DROP TABLE semantic.semantic_municipios_dic;")
dbWriteTable(con, c("semantic",'semantic_municipios_dic'),municipios_dic, row.names=FALSE)




dbGetQuery(con, "DROP TABLE raw.semantic_municipios_dic;")
dbWriteTable(con, c("raw",'semantic_municipios_dic'),municipios_dic, row.names=FALSE)
