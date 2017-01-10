###############################################################################
# CSV to RAW 
# ETL's en R para migrar las bases de datos a postgres.
###############################################################################

rm(list=ls())
suppressPackageStartupMessages({
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
})


##################
## Create Connection to DB
##################
conf <- fromJSON("../conf/conf_profile.json")
pg = dbDriver("PostgreSQL")
con = dbConnect(pg, user=conf$PGUSER, password=conf$PGPASSWORD,
                host=conf$PGHOST, port=5432, dbname=conf$PGDATABASE)


##################
## CENAPRED (APP)
# www.atlasnacionalderiesgos.gob.mx
##################
cenapred <-  read_csv("../data/cenapred/cenapred_app.csv")
cenapred_dic <-  read_csv("../data/cenapred/cenapred_app_dic.csv")
dbWriteTable(con, c("raw",'cenapred_app_municipios'),cenapred, row.names=FALSE)
dbWriteTable(con, c("raw",'cenapred_app_municipios_dic'),cenapred, row.names=FALSE)




##################
## INPC (INEGI)
# http://www.inegi.org.mx/sistemas/bie/?idserPadre=11100070001000100050#D11100070001000100050
##################
INPC  <- read_csv("../data/INPC/BIE_BIE20170105193841.csv") %>%
  rename(date=Periodo) %>% mutate(date=as_datetime(as.yearmon(date,"%Y/%m")))
colnames(INPC) <- dbSafeNames(colnames(INPC))

#dbWriteTable(con, c("raw",'inpc_ciudades'),INPC, row.names=FALSE)

INPC = as_tibble(dbGetQuery(con, "select * from raw.inpc_ciudades;"))
INPC_colname  <- read_csv("../data/INPC/dict_ciudades.csv") %>% 
  select(cve_ent) %>% as.list() 
colnames(INPC) <- c(colnames(INPC)[1],INPC_colname$cve_ent)

names <- colnames(INPC)[2:length(colnames(INPC))]
INPC.t <- t(INPC[nrow(INPC),2:ncol(INPC)]) %>% as_tibble() %>%
  rename(INPC=V1)
INPC.t["cve_ent"] <- names
INPC <- INPC.t %>% group_by(cve_ent) %>% summarise(INPC=mean(INPC))

#scale(INPC$INPC)
#scale(data.Normalization(INPC$INPC,type="n1",normalization="column"))
+#n1 - standardization ((x-mean)/sd)


##################
## INDICE REZAGO SOCIAL (CONEVAL)
# http://www.coneval.org.mx/Medicion/IRS/Paginas/Indice_Rezago_Social_2015.aspx
##################

IRS_mun <- readxl::read_excel("../data/Tablero/IRS/IRS_2000_2015_vf.xlsx",sheet = "Municipios",skip = 5,
                              col_names = FALSE) %>% head(2457) %>%
  rename(cve_edo = X0, nom_edo=X1,cve_muni=X2, nom_muni=X3, 
         pobtot_00 = X4, pobtot_05 = X5, pobtot_10 = X6, pobtot_15 = X7, 
         porc_pob_15_analfa00 = X8,porc_pob_15_analfa05 = X9,porc_pob_15_analfa10 = X10,porc_pob_15_analfa15 = X11,
         porc_pob614_noasiste00 = X12,porc_pob614_noasiste05 = X13,porc_pob614_noasiste10 = X14,porc_pob614_noasiste15 = X15,
         porc_pob15_basicainc00 = X16,porc_pob15_basicainc05 = X17,porc_pob15_basicainc10 = X18,porc_pob15_basicainc15 = X19,
         porc_pob_snservsal00 = X20,porc_pob_snservsal05 = X21,porc_pob_snservsal10 = X22,porc_pob_snservsal15 = X23,
         porc_vivpisotierra00 = X24,porc_vivpisotierra05 = X25,porc_vivpisotierra10 = X26,porc_vivpisotierra15 = X27,
         porc_vivsnsan00 = X28,porc_vivsnsan05 = X29,porc_vivsnsan10 = X30,porc_vivsnsan15 = X31,
         porc_snaguaent00 = X32,porc_snaguaent05 = X33,porc_snaguaent10 = X34,porc_snaguaent15 = X35,
         porc_vivsndren00 = X36,porc_vivsndren05 = X37,porc_vivsndren10 = X38,porc_vivsndren15 = X39,
         porc_vivsnenergia00 = X40,porc_vivsnenergia05 = X41,porc_vivsnenergia10 = X42,porc_vivsnenergia15 = X43,
         porc_vivsnlavadora00 = X44,porc_vivsnlavadora05 = X45,porc_vivsnlavadora10 = X46,porc_vivsnlavadora15 = X47,
         porc_vivsnrefri00 = X48,porc_vivsnrefri05 = X49,porc_vivsnrefri10 = X50,porc_vivsnrefri15 = X51,
         irez_soc00 = X52,irez_soc05 = X53,irez_soc10 = X54,irez_soc15 = X55,
         gdo_rezsoc00 = X56,gdo_rezsoc05 = X57,gdo_rezsoc10 = X58,gdo_rezsoc15 = X59,
         l_ocupnac00 = X60,l_ocupnac05 = X61,l_ocupnac10 = X62,l_ocupnac15 = X63)

dbWriteTable(con, c("raw",'ISR_municipios'),IRS_mun, row.names=FALSE)



IRS_edo <- readxl::read_excel("../data/Tablero/IRS/IRS_2000_2015_vf.xlsx",sheet = "Estados",skip = 5,
                              col_names = FALSE) %>% head(34) %>%
  rename(cve_edo = X0, nom_edo=X1, 
         pobtot_00 = X2, pobtot_05 = X3, pobtot_10 = X4, pobtot_15 = X5, 
         porc_pob_15_analfa00 = X6,porc_pob_15_analfa05 = X7,porc_pob_15_analfa10 = X8,porc_pob_15_analfa15 = X9,
         porc_pob614_noasiste00 = X10,porc_pob614_noasiste05 = X11,porc_pob614_noasiste10 = X12,porc_pob614_noasiste15 = X13,
         porc_pob15_basicainc00 = X14,porc_pob15_basicainc05 = X15,porc_pob15_basicainc10 = X16,porc_pob15_basicainc15 = X17,
         porc_pob_snservsal00 = X18,porc_pob_snservsal05 = X19,porc_pob_snservsal10 = X20,porc_pob_snservsal15 = X21,
         porc_vivpisotierra00 = X22,porc_vivpisotierra05 = X23,porc_vivpisotierra10 = X24,porc_vivpisotierra15 = X25,
         porc_vivsnsan00 = X26,porc_vivsnsan05 = X27,porc_vivsnsan10 = X28,porc_vivsnsan15 = X29,
         porc_snaguaent00 = X30,porc_snaguaent05 = X31,porc_snaguaent10 = X32,porc_snaguaent15 = X33,
         porc_vivsndren00 = X34,porc_vivsndren05 = X35,porc_vivsndren10 = X36,porc_vivsndren15 = X37,
         porc_vivsnenergia00 = X38,porc_vivsnenergia05 = X39,porc_vivsnenergia10 = X40,porc_vivsnenergia15 = X41,
         porc_vivsnlavadora00 = X42,porc_vivsnlavadora05 = X43,porc_vivsnlavadora10 = X44,porc_vivsnlavadora15 = X45,
         porc_vivsnrefri00 = X46,porc_vivsnrefri05 = X47,porc_vivsnrefri10 = X48,porc_vivsnrefri15 = X49,
         irez_soc00 = X50,irez_soc05 = X51,irez_soc10 = X52,irez_soc15 = X53,
         gdo_rezsoc00 = X54,gdo_rezsoc05 = X55,gdo_rezsoc10 = X56,gdo_rezsoc15 = X57,
         l_ocupnac00 = X58,l_ocupnac05 = X59,l_ocupnac10 = X60,l_ocupnac15 = X61)

dbWriteTable(con, c("raw",'ISR_estados'),IRS_edo, row.names=FALSE)

IRS_dic <- read_csv("../data/Tablero/IRS/IRS_dic.csv")
dbWriteTable(con, c("raw",'ISR_dic'),IRS_dic, row.names=FALSE)



##########################
####PERSONAL MÉDICO (INAFED)
# http://www.inafed.gob.mx/es/inafed/Principales_Datos_Socioeconomicos_por_Municipio
##########################

poblacion = as_tibble(dbGetQuery(con, "select cve_muni, pobtot_ajustada from raw.semantic_municipios;"))
medicos <- read_csv("../data/inafed/personal_medico.csv") %>% mutate(cve_muni = str_pad(cve_muni,width = 5,pad ="0" ))
medicos <- medicos %>% left_join(poblacion) %>% mutate(personal_medico_100_h_2012 = (personal_medico_2012/pobtot_ajustada) * 100000)
medicos$personal_medico_100_h_2012 <-replace(medicos$personal_medico_100_h_2012,which(is.na(medicos$personal_medico_100_h_2012)),0)
medicos <- medicos %>% mutate(personal_medico_100_h_2012_norm = normalize(personal_medico_100_h_2012))
dbWriteTable(con, c("raw",'personal_medico'),medicos, row.names=FALSE)
medicos_dic <- read_csv("../data/inafed/personal_medico_dic.csv")
dbWriteTable(con, c("raw",'personal_medico_dic'),medicos_dic, row.names=FALSE)



##########################
####INFRAESTRUCTURA SOCIAL (SEDESOL)
# SEDESOL 2016
##########################

diconsa = read_csv("../data/Tablero/20161116MIER1900/08INFRASOCIAL/DICONSA/V_IIS_DIC_L.csv")  %>%
  select(CVE_LOCC, TOT_TIENDAS) %>% rename(cve_locc=CVE_LOCC,tot_diconsa=TOT_TIENDAS)
comedores = read_csv("../data/Tablero/20161116MIER1900/08INFRASOCIAL/COMEDORES_COMUN/V_IIS_COM_L.csv")  %>%
  select(CVE_LOCC, TOT_COME) %>% rename(cve_locc=CVE_LOCC,tot_comedores=TOT_COME)
estancias = read_csv("../data/Tablero/20161116MIER1900/08INFRASOCIAL/ESTANCIAS_INF/V_IIS_ESTINF_L.csv")  %>%
  select(CVE_LOCC, TOT_ESTANCIAS) %>% rename(cve_locc=CVE_LOCC,tot_estancias=TOT_ESTANCIAS)
liconsa = read_csv("../data/Tablero/20161116MIER1900/08INFRASOCIAL/LICONSA/V_IIS_LIC_L.csv")  %>%
  select(CVE_LOCC, TOT_LECHE) %>% rename(cve_locc=CVE_LOCC,tot_liconsa=TOT_LECHE)
infraestructura_social <- full_join(diconsa,comedores) %>% full_join(estancias) %>% full_join(liconsa)
infraestructura_social <- infraestructura_social %>% mutate(cve_muni = str_extract(cve_locc,"[0-9]{5}$")) 
dbWriteTable(con, c("raw",'inf_social_localidades'),infraestructura_social, row.names=FALSE)
infraestructura_social_dic <- read_csv("../data/Tablero/20161116MIER1900/08INFRASOCIAL/infra_social_dic.csv") 
dbWriteTable(con, c("raw",'inf_social_localidades_dic'),infraestructura_social_dic, row.names=FALSE)



##########################
#### PUB LOCALIDADES 
##########################

pub_localidades = as_tibble(dbGetQuery(con, "select * from raw.pub_localidades;"))
pub_localidades <- read_csv("../data/Tablero/localidad/pub_localidades.csv") 
pub_localidades[is.na(pub_localidades)] <- 0
summary(pub_localidades)
dbWriteTable(con, c("raw",'pub_localidades'),pub_localidades, row.names=FALSE)

pub_localidades <- read_csv("../data/Tablero/localidad/pub_localidades.csv")



##########################
#### TASA DE HOMICIDIOS 2010 
##########################
#homicidio <- read_csv("/home/baco/Dropbox/Tesis_ITAM_RS/Base_Tesis.csv") %>% filter(year==2010) %>% select(code, hom_rate_riv) %>% 
#  rename(cve_muni=code) %>%   mutate(cve_muni = str_pad(cve_muni,width = 5,pad = "0")) 
#write.csv(homicidio,"../data/incidencia_delictiva/homicidios.csv",row.names = FALSE)  


fuero_comun <- read_csv("../data/incidencia_delictiva/fuero_comun_diego_valle/fuero-comun-municipios.csv")
fuero_comun <- fuero_comun %>% mutate(date=as_date(as.yearmon(date,"%Y-%m")))
dbWriteTable(con, c("raw",'fuero_comun_municipios'),fuero_comun, row.names=FALSE)


################################################################
################################################################
# CONEVAL ESTATAL
# Este script toma las bases de datos limpias de indicadores de coneval y sube a la base de datos estandarizadas a
# postgres.
# Lo hace para los años 
# 2010, 2012 y 2014 Obtenidas de: http://www.coneval.org.mx/Medicion/MP/Paginas/Programas_BD_10_12_14.aspx

#Para el caso 2010 la base se consiguió de otro lado porque el código de R de coneval tenía muchos bugs

################################################################
################################################################

################################
# CONEVAL ESTATAL 2010
################################

estatal_10 <- as_tibble(read.csv("../data/coneval/raw_estatal/Indicadores_10.csv",colClasses = c("character",rep('numeric',32))))
colnames(estatal_10) <- paste(colnames(estatal_10),"10", sep = "_")
estatal_10 <- dplyr::rename(estatal_10,cve_ent=cve_ent_10)

################################
# CONEVAL ESTATAL 2012
################################

tabstat_sum <- as_tibble(read.csv("../data/coneval/raw_estatal/Indicadores_total_12.csv",encoding = "latin1"))
tabstat_mean<- as_tibble(read.csv("../data/coneval/raw_estatal/Indicadores_mean_12.csv",encoding = "latin1"))
colnames(tabstat_sum) <- paste(colnames(tabstat_sum),"n", sep = "_")
colnames(tabstat_mean) <- paste(colnames(tabstat_mean),"p", sep = "_")
colnames(tabstat_sum) <- paste(colnames(tabstat_sum),"12", sep = "_")
colnames(tabstat_mean) <- paste(colnames(tabstat_mean),"12", sep = "_")
tabstat_sum <- mutate(tabstat_sum,cve_ent= str_pad(row.names(tabstat_sum),width = 2,pad="0"))
tabstat_sum$X_n_12 <- NULL
tabstat_mean <- mutate(tabstat_mean,cve_ent= str_pad(row.names(tabstat_sum),width = 2,pad="0")) 
tabstat_mean$X_p_12 <- NULL
estatal_12 <- left_join(tabstat_sum, tabstat_mean)

################################
# CONEVAL ESTATAL 2014
################################

tabstat_sum <- as_tibble(read.csv("../data/coneval/raw_estatal/Indicadores_total_14.csv",encoding = "utf-8"))
tabstat_mean<- as_tibble(read.csv("../data/coneval/raw_estatal/Indicadores_mean_14.csv",encoding = "utf-8"))
colnames(tabstat_sum) <- paste(colnames(tabstat_sum), "n", sep = "_")
colnames(tabstat_mean) <- paste(colnames(tabstat_mean), "p", sep = "_")
colnames(tabstat_sum) <- paste(colnames(tabstat_sum),"14", sep = "_")
colnames(tabstat_mean) <- paste(colnames(tabstat_mean),"14", sep = "_")

tabstat_sum <- mutate(tabstat_sum,cve_ent = str_pad(row.names(tabstat_sum),width = 2,pad="0"))
tabstat_sum$X_n_14 <- NULL
tabstat_mean <- mutate(tabstat_mean,cve_ent= str_pad(row.names(tabstat_sum),width = 2,pad="0"))
tabstat_mean$X_p_14 <- NULL
estatal_14 <- left_join(tabstat_sum, tabstat_mean)



################################
# CONEVAL CLEAN 2010 - 2014
################################

estatal <- left_join(estatal_10,estatal_12) %>% left_join(estatal_14)
estatal_dic <- read_csv("../data/coneval/clean_estatal/coneval_estatal_dic.csv")

dbWriteTable(con, c("raw",'coneval_estados'),estatal, row.names=FALSE)
dbWriteTable(con, c("raw",'coneval_estados_dic'),estatal_dic, row.names=FALSE)


##################
## Disconnect to DB
##################

dbDisconnect(con)