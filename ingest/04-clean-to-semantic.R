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
## Semantic localidades
##################

municipios = as_tibble(dbGetQuery(con, "select * from semantic.semantic_municipios;"))
colnames(municipios) = dbSafeNames(colnames(municipios))
municipios<-round_df(municipios, digits=3)

municipios<-municipios %>% dplyr::select(cve_muni,nom_mun,cve_ent,pobreza_p_10, pobreza_m_p_10, pobreza_e_p_10, vul_car_p_10, vul_ing_p_10, no_pobv_p_10, pobreza_n_10, pobreza_m_n_10, pobreza_e_n_10, vul_car_n_10, vul_ing_n_10, no_pobv_n_10,p_0196,e016,s176,e003,s061,s052,s065,s071,s174,s017,p_0424,s216,s057,s072,s241,gini_90,gini_00,gini_10,irez_soc10,irez_soc00,irez_soc05,irez_soc15,pobtot_15,porc_pob_snservsal15,porc_pob_15_analfa15,porc_snaguaent15,porc_vivsndren15,porc_vivsnenergia15,porc_vivsnsan15,carencias_n_10,ic_ali_n_10,ic_segsoc_n_10,ic_sbv_n_10,ic_asalud_n_10,ic_cev_n_10,plb_n_10,ic_rezedu_n_10,carencias3_n_10,pobreza_n_10,pobreza_e_n_10,pobreza_m_n_10,no_pobv_n_10,vul_car_n_10,vul_ing_n_10,plb_p_10,carencias_p_10,ic_ali_p_10,ic_segsoc_p_10,ic_sbv_p_10,ic_asalud_p_10,ic_cev_p_10,ic_rezedu_p_10)
colnames(municipios) <- dbSafeNames_explorador(colnames(municipios),9)




dbGetQuery(con, "DROP TABLE clean.semantic_municipios;")
dbWriteTable(con, c("clean",'semantic_municipios'),municipios, row.names=FALSE)
#dbGetQuery(con, "DROP TABLE clean.semantic_municipios_dic;")


municipios_dic = as_tibble(dbGetQuery(con, "select * from semantic.semantic_municipios_dic;"))
municipios_dic$id <- dbSafeNames(municipios_dic$id)
municipios_dic$id <- dbSafeNames_explorador(municipios_dic$id,n)
write_csv(municipios_dic,"mun_dic_temp.csv")

# Menos Variables
municipios_dic<- read_csv("mun_dic_temp_2.csv")
municipios_dic$id <- dbSafeNames(municipios_dic$id)
municipios_dic$id <- dbSafeNames_explorador(municipios_dic$id,n)

dbGetQuery(con, "DROP TABLE clean.semantic_municipios_dic;")
dbWriteTable(con, c("clean",'semantic_municipios_dic'),municipios_dic, row.names=FALSE)
