###############################################################################
# Clean to Explorador Municipios y Estados 

#Script to create semantic.explorador_* tables - explorador module
#Dependence: S3-to-raw.sh
#            raw-to-clean-monitor

#Assumes existence of:
#  Municipios
#    clean.explorador_municipios + dic
#  Estados
#    clean.explorador_estados + dic
###############################################################################

rm(list=ls())
source("./utilsAPI.R")


##################
## Explorador municipios
##################
municipios = as_tibble(dbGetQuery(con, "select * from clean.explorador_municipios;"))

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


municipios <- municipios %>% dplyr::mutate_each(funs(as.numeric(.)),matches("^[s|p[_]|e|u][0-9]+", ignore.case=FALSE))
dbGetQuery(con, "DROP TABLE semantic.explorador_municipios;")
dbWriteTable(con, c("semantic",'explorador_municipios'),municipios, row.names=FALSE)

##################
## Explorador DIC municipios
##################

clean_dic <- colnames(municipios)
municipios_dic = as_tibble(dbGetQuery(con, "select * from clean.explorador_municipios_dic;"))
municipios_dic$id = dbSafeNames(municipios_dic$id)
municipios_dic <- municipios_dic %>% filter(municipios_dic$id %in% clean_dic)
#write_csv(municipios_dic,"semantic_municipios_dic.csv")
#municipios_dic<- read_csv("semantic_municipios_dic.csv")
dbGetQuery(con, "DROP TABLE semantic.explorador_municipios_dic;")
dbWriteTable(con, c("semantic",'explorador_municipios_dic'),municipios_dic, row.names=FALSE)



##################
## Explorador Estados
##################
estados = as_tibble(dbGetQuery(con, "select * from clean.explorador_estados;"))
colnames(estados) = dbSafeNames(colnames(estados))
estados<-round_df(estados, digits=3)

#select variables
#estados<-estados %>% dplyr::select()

estados <- estados %>% dplyr::mutate_each(funs(as.numeric(.)),matches("^[s|p[_]|n[_]|e|u][0-9]+", ignore.case=FALSE))
dbGetQuery(con, "DROP TABLE semantic.explorador_estados;")
dbWriteTable(con, c("semantic",'explorador_estados'),estados, row.names=FALSE)
clean_dic <- colnames(estados)

##################
## Explorador DIC Estados
##################

estados_dic = as_tibble(dbGetQuery(con, "select * from clean.explorador_estados_dic;"))
estados_dic$id = dbSafeNames(estados_dic$id)
estados_dic <- estados_dic %>% filter(estados_dic$id %in% clean_dic)
dbGetQuery(con, "DROP TABLE semantic.explorador_estados_dic;")
dbWriteTable(con, c("semantic",'explorador_estados_dic'),estados_dic, row.names=FALSE)
