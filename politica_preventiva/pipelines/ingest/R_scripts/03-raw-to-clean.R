###############################################################################
# RAW to Clean 
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
## semantic localidades
##################
#sematic_localidades = as_tibble(dbGetQuery(con, "select * from raw.semantic_localidades;"))

#dbGetQuery(con, "DROP TABLE clean.amenazas_naturales_municipios;")
#dbWriteTable(con, c("clean",'amenazas_naturales_municipios'),amenazas_naturales, row.names=FALSE)


##################
## road density
##################

red_vial <- readOGR("../data/shp/red_vial/raw/","red_vial_2015")
lu <- data.frame()
lu <- rbind(lu, red_vial@data)

lu$ID_RED <- as.character(lu$ID_RED)
lu$country <- NA
lu$country <- "mexico"

# Merge lu (LookUp) into polygons,
red_vial@data$ID_RED <- as.character(red_vial@data$ID_RED)
red_vial@data <- full_join(red_vial@data, lu, by = "ID_RED")

# Tidy merged data
red_vial@data <- red_vial@data[c("ID_RED", "country")]
colnames(red_vial@data) <- c("ID_RED", "country")

# Ensure shapefile row.names and polygon IDs are sensible
row.names(red_vial) <- row.names(red_vial@data)
red_vial <- spChFIDs(red_vial, row.names(red_vial))

red_vial <- gUnaryUnion(red_vial, id = red_vial@data$country)

plot(red_vial)

##################
## AMENAZAS HUMANAS VIOLENCIA
#Estado y municipios
##################
# LOAD FROM RAW
#muy pesado
#fuero_comun = as_tibble(dbGetQuery(con, "select * from raw.fuero_comun_municipios;"))

## Clean ##
#homicidios
  #Con arma blanca
  #Con arma de fuego
  #Otros
    #dolosos
    #culposos

temp<-fuero_comun %>% mutate(cve_ent = str_pad(state_code,width = 2,pad="0"),
                             cve_mun = str_pad(mun_code,width = 3,pad="0")) %>%
  mutate(cve_muni = str_c(cve_ent,cve_mun)) %>%
  dplyr::filter(modalidad=="HOMICIDIOS") 


homicidios_ent <- temp %>% group_by(cve_ent) %>%
  dplyr::summarise(homicidios_totales = sum(count,na.rm = TRUE), 
                   POB_TOT = mean(population)) %>%
  rename(amenazas_humana_violencia = homicidios_totales) %>%
  mutate(amenazas_humana_violencia_i = amenazas_humana_violencia/POB_TOT*100000)

homicidios_muni <- temp %>% group_by(cve_muni) %>%
  dplyr::summarise(homicidios_totales = sum(count,na.rm = TRUE), 
                   POB_TOT = mean(population)) %>%
  rename(amenazas_humana_violencia = homicidios_totales) %>%
  mutate(amenazas_humana_violencia_i = amenazas_humana_violencia/POB_TOT*100000)


# LOG homicidios
homicidios_ent$amenazas_humana_violencia_i <- log(homicidios_ent$amenazas_humana_violencia_i)
homicidios_muni$amenazas_humana_violencia_i <- log(homicidios_muni$amenazas_humana_violencia_i)

# Obten el mínimo no infinito
min_homicidio_ent <- homicidios_ent %>% dplyr::select(amenazas_humana_violencia_i) %>% 
  dplyr::filter(is.infinite(amenazas_humana_violencia_i)==FALSE) %>% 
  min()

min_homicidio_muni <- homicidios_muni %>% dplyr::select(amenazas_humana_violencia_i) %>% 
  dplyr::filter(is.infinite(amenazas_humana_violencia_i)==FALSE) %>% 
  min()


# sustituye infinito por algo más pequeño
homicidios_muni$amenazas_humana_violencia_i[is.infinite(homicidios_muni$amenazas_humana_violencia_i)]<- min_homicidio_muni - 1
homicidios_ent$amenazas_humana_violencia_i[is.infinite(homicidios_ent$amenazas_humana_violencia_i)]<- min_homicidio_ent - 1

homicidios_ent$amenazas_humana_violencia_i <- scale(homicidios_ent$amenazas_humana_violencia_i)
homicidios_muni$amenazas_humana_violencia_i <- scale(homicidios_muni$amenazas_humana_violencia_i)

summary(homicidios_ent$amenazas_humana_violencia_i)
summary(homicidios_muni$amenazas_humana_violencia_i)

# LOAD TO CLEAN
dbWriteTable(con, c("clean",'amenazas_humana_violencia_municipios'),homicidios_muni, row.names=FALSE)
dbWriteTable(con, c("clean",'amenazas_humana_violencia_estados'),homicidios_ent, row.names=FALSE)


##########################
#### Amenazas Humanas
##########################

volatilidad_anual = as_tibble(dbGetQuery(con, 
                            "select * from raw.ipa_food_price where año >= 2014;"))

volatilidad_anual<-volatilidad_anual %>% group_by(estado,año) %>% 
  summarise(Q_n = sum(ipa_n,na.rm = TRUE),n =n()) %>% group_by(estado) %>%
  summarise(a_h_volatilidad_i = median(Q_n,na.rm = TRUE))  %>% 
  mutate(a_h_volatilidad_i = normalize(a_h_volatilidad_i))

# 04 Campeche- Tabasca
campeche = volatilidad_anual[volatilidad_anual$estado=="27", ]["a_h_volatilidad_i"]  %>% unlist(use.names = FALSE)
# 06 colima-Jalisco
colima = volatilidad_anual[volatilidad_anual$estado=="14", ]["a_h_volatilidad_i"]  %>% unlist(use.names = FALSE)
# 12 guerrero-oaxaca
guerrero = volatilidad_anual[volatilidad_anual$estado=="20", ]["a_h_volatilidad_i"]  %>% unlist(use.names = FALSE)
# 17 morelos-mexico
morelos = volatilidad_anual[volatilidad_anual$estado=="15", ]["a_h_volatilidad_i"]  %>% unlist(use.names = FALSE)
# 23 quintana-roo-yucatan
quintana_roo = volatilidad_anual[volatilidad_anual$estado=="31", ]["a_h_volatilidad_i"]  %>% unlist(use.names = FALSE)
# 29 tlaxcala-puebla
tlaxcala = volatilidad_anual[volatilidad_anual$estado=="21", ]["a_h_volatilidad_i"]  %>% unlist(use.names = FALSE)

volatilidad_anual <- volatilidad_anual %>% 
  add_row(estado = "04", a_h_volatilidad_i = campeche) %>%
  add_row(estado = "06", a_h_volatilidad_i = colima) %>%
  add_row(estado = "12", a_h_volatilidad_i = guerrero) %>%
  add_row(estado = "17", a_h_volatilidad_i = morelos) %>%
  add_row(estado = "23", a_h_volatilidad_i = quintana_roo) %>%
  add_row(estado = "29", a_h_volatilidad_i = tlaxcala) %>%
  rename(cve_ent=estado)

INPC = as_tibble(dbGetQuery(con, "select * from raw.inpc_estados where fecha ='2016-9';")) %>% 
  dplyr::select(cve_ent,inpc)



# estados


homicidios_ent = as_tibble(dbGetQuery(con, "select * from clean.amenazas_humana_violencia_estados;")) 

amenazas_humanas_ent <- homicidios_ent %>% left_join(volatilidad_anual) %>% left_join(INPC) %>% 
  rename(a_h_e_inpc_i=inpc) %>%  mutate(amenazas_humana_i = (amenazas_humana_violencia_i*a_h_volatilidad_i*a_h_e_inpc_i)**(1/3)) %>%
  dplyr::select(-POB_TOT)

dbGetQuery(con, "DROP TABLE clean.amenazas_humanas_estados;")
dbWriteTable(con, c("clean",'amenazas_humanas_estados'),amenazas_humanas_ent, row.names=FALSE)

# municipios
homicidios_muni = as_tibble(dbGetQuery(con, "select * from clean.amenazas_humana_violencia_municipios;")) %>%
  mutate(cve_ent = str_sub(cve_muni,0,2))


amenazas_humanas_muni <- homicidios_muni %>% left_join(volatilidad_anual) %>% left_join(INPC) %>% 
  rename(a_h_e_inpc_i=inpc) %>%  mutate(amenazas_humana_i = (amenazas_humana_violencia_i*a_h_volatilidad_i*a_h_e_inpc_i)**(1/3)) %>%
  dplyr::select(-POB_TOT)

dbGetQuery(con, "DROP TABLE clean.amenazas_humanas_municipios;")
dbWriteTable(con, c("clean",'amenazas_humanas_municipios'),amenazas_humanas_muni, row.names=FALSE)



##################
## AMENAZAS NATURALES
##################

# Municipal
# LOAD FROM RAW
amenazas_naturales = as_tibble(dbGetQuery(con, 
      "select * from raw.cenapred_app_municipios;")) %>% dplyr::select(cve_muni,POBTOT_,GP_BajasTe, GP_Ciclnes, GP_Granizo, GP_Inundac, 
                                                                       GP_Nevadas, GP_Sequia2, GP_Sismico, GP_SusInfl, 
                                                                       GP_SusTox, GP_TormEle, GP_Tsunami, GP_ondasca, 
                                                                       GVS_ISE10, G_SusceLad)

alerta_produccion = as_tibble(dbGetQuery(con, 
                                          "select cve_mun as cve_muni, stdev as a_n_produccion_i
                                         from raw.semantic_alerta_produccion_15;")) %>%
  dplyr::mutate(a_n_produccion_i = 
                  ifelse(is.na(a_n_produccion_i), mean(a_n_produccion_i,na.rm = T), a_n_produccion_i)) %>%
  dplyr::mutate(a_n_produccion_i = normalize(scale(-1 * a_n_produccion_i, center = TRUE, scale = FALSE))) 

# %>%ggplot(aes(a_n_produccion_i)) + geom_density()

amenazas_naturales_i <- amenazas_naturales %>%
  mutate_each(funs(recode(.,"Muy alto"=10, "Alto" = 8, 
                          "Medio"=4, "Bajo"=2, "Muy bajo"= 1,"Sin peligro"=0,"Nd"=0)), matches("^G.*", ignore.case=FALSE))
amenazas_naturales_i <- amenazas_naturales_i %>%
  mutate_each(funs(normalize(scale(.))), matches("^G.*", ignore.case=FALSE))

#summary(amenazas_naturales)
#ggplot(amenazas_naturales,aes(G_SusceLad)) + geom_histogram()
colnames(amenazas_naturales_i) = dbSafeNames(colnames(amenazas_naturales_i))
colnames(amenazas_naturales_i) = paste0("a_n_", colnames(amenazas_naturales_i),"_i")

amenazas_naturales_i <- amenazas_naturales_i %>% rename(cve_muni = a_n_cve_muni_i) %>% 
  left_join(alerta_produccion)

# Imputation
# Inspection of the missing data:
md.pattern(amenazas_naturales_i)
tempData <- mice(amenazas_naturales_i,m=5,maxit=50,meth='pmm',seed=500)
summary(tempData)
completedData <- complete(tempData,'long')
impute<-aggregate(completedData[,19] , by = list(completedData$cve_muni),FUN= mean) %>%
  rename(cve_muni = Group.1, a_n_produccion_i= V1)


amenazas_naturales_i["a_n_produccion_i"]<-NULL
amenazas_naturales_i <- left_join(amenazas_naturales_i,impute)
summary(amenazas_naturales_i)



colnames(amenazas_naturales) = dbSafeNames(colnames(amenazas_naturales))
colnames(amenazas_naturales) = paste0("a_n_", colnames(amenazas_naturales))
amenazas_naturales <- amenazas_naturales %>% rename(cve_muni = a_n_cve_muni)


amenazas_naturales <- amenazas_naturales %>% left_join(amenazas_naturales_i)

amenazas_naturales$amenazas_naturales_i <-  rowMeans(subset(amenazas_naturales, 
                                                                select = c("a_n_produccion_i", "a_n_gp_inundac_i","a_n_gp_sequia2_i",
                                                                           "a_n_gp_tsunami_i","a_n_gp_sismico_i","a_n_gp_bajaste_i" ,
                                                                           "a_n_gp_ciclnes_i","a_n_g_suscelad_i"), na.rm = TRUE))


summary(amenazas_naturales)
colnames(amenazas_naturales)
dbGetQuery(con, "DROP TABLE clean.amenazas_naturales_municipios;")
dbWriteTable(con, c("clean",'amenazas_naturales_municipios'),amenazas_naturales, row.names=FALSE)


#Estatal

amenazas_naturales_ent = as_tibble(dbGetQuery(con, 
                                          "select * from clean.amenazas_naturales_municipios;"))%>%
  mutate(cve_ent = str_sub(cve_muni,0,2))

amenazas_naturales_ent <- amenazas_naturales_ent %>% dplyr::select(cve_ent
                                                            ,a_n_pobtot
                                                            ,a_n_gp_bajaste_i
                                                            ,a_n_gp_ciclnes_i
                                                            ,a_n_gp_granizo_i
                                                            ,a_n_gp_inundac_i
                                                            ,a_n_gp_nevadas_i
                                                            ,a_n_gp_sequia2_i
                                                            ,a_n_gp_sismico_i
                                                            ,a_n_gp_susinfl_i
                                                            ,a_n_gp_sustox_i 
                                                            ,a_n_gp_tormele_i
                                                            ,a_n_gp_tsunami_i
                                                            ,a_n_gp_ondasca_i
                                                            ,a_n_gvs_ise10_i 
                                                            ,a_n_g_suscelad_i,
                                                            a_n_produccion_i) %>% 
  rename(a_n_produccion=a_n_produccion_i) 

tmp<- amenazas_naturales_ent %>% group_by(cve_ent) %>% 
  summarise(a_n_produccion =  median(a_n_produccion,na.rm = T)) %>%
  ungroup() %>% rename(a_n_produccion_i=a_n_produccion)

amenazas_naturales_ent <- amenazas_naturales_ent %>%
  mutate_each(funs(as.integer(.)*a_n_pobtot), ends_with("_i")) %>%
  ungroup()


amenazas_naturales_ent <- amenazas_naturales_ent %>% 
  group_by(cve_ent) %>% summarise_each(funs(sum),ends_with("_i")) %>%
  ungroup() %>% mutate_each(funs(normalize),ends_with("_i"))

amenazas_naturales_ent <- amenazas_naturales_ent %>% left_join(tmp)

summary(amenazas_naturales_ent)

amenazas_naturales_ent$amenazas_naturales_i <-  rowMeans(subset(amenazas_naturales_ent, 
        select = c("a_n_produccion_i", "a_n_gp_inundac_i","a_n_gp_sequia2_i",
                   "a_n_gp_tsunami_i","a_n_gp_sismico_i","a_n_gp_bajaste_i" ,
                   "a_n_gp_ciclnes_i","a_n_g_suscelad_i"), na.rm = TRUE))
                                      
#library(ggplot2);library(reshape2)
#data<- melt(tp)
#ggplot(data,aes(x=value, fill=variable)) + geom_density(alpha=0.25)
#ggplot(data,aes(x=value, fill=variable)) + geom_histogram(alpha=0.25)
#ggplot(data,aes(x=variable, y=value, fill=variable)) + geom_boxplot()

# LOAD TO CLEAN
dbGetQuery(con, "DROP TABLE clean.amenazas_naturales_estados;")
dbWriteTable(con, c("clean",'amenazas_naturales_estados'),amenazas_naturales_ent, row.names=FALSE)


##########################
#### AMENAZAS HUMANAS
##########################

amenazas_naturales_ent = as_tibble(dbGetQuery(con, 
                                              "select * from clean.amenazas_naturales_municipios;"))%>%
  mutate(cve_ent = str_sub(cve_muni,0,2))

ipa_volatilidad = as_tibble(dbGetQuery(con, 
                                              "select * from raw.ipa_food_price;"))

%>%
  mutate(cve_ent = str_sub(cve_muni,0,2))










##########################
#### PUB ESTADOS 
##########################
# aún no está este proceso
# modifiqué los nombres de los programas por n_# y minúsculas"
pub_estados <- read_csv("../data/Tablero/pub/pub_estados.csv") 
dbWriteTable(con, c("clean",'pub_estados'),pub_estados, row.names=FALSE)


##########################
#### PUB LOCALIDADES 
##########################
# LOAD FROM csv
pub_localidades = as_tibble(dbGetQuery(con, 
                                          "select * from raw.pub_localidades;"))
a = colnames(pub_localidades)
a[2] = "n_0196"
a[3] = "n_0342"
colnames(pub_localidades) = a
write.csv(pub_localidades,"../data/Tablero/pub/pub_localidades_raw.csv",row.names = FALSE)
dbWriteTable(con, c("clean",'pub_localidades'),pub_localidades, row.names=FALSE)

##########################
#### PUB MUNICIPIOS
##########################
# LOAD FROM csv
pub_municipios = as_tibble(dbGetQuery(con, 
                                       "select * from raw.pub_municipios;"))
a = colnames(pub_municipios)
a[3] = "p_0424"
a[16] = "p_0196"
a[19] = "p_0342"
colnames(pub_municipios) = a
# write.csv(pub_municipios,"../data/Tablero/pub/pub_municipios_raw.csv",row.names = FALSE)
dbGetQuery(con, "DROP TABLE clean.pub_municipios;")
dbWriteTable(con, c("clean",'pub_municipios'),pub_municipios, row.names=FALSE)

##########################
#### PUB dict 
##########################

pub_municipios_dic = as_tibble(dbGetQuery(con, 
                                          "select * from clean.semantic_municipios_dic;"))
write_csv(pub_municipios_dic,"pub_municipios_dic.csv")


pub_municipios_dic = as_tibble(dbGetQuery(con, 
                                      "select * from clean.pub_diccionario_programas;"))
pub_municipios_dic$cve_prog_dggpb <- dbSafeNames(pub_municipios_dic$cve_prog_dggpb)

write_csv(pub_municipios_dic,"pub_municipios_dic.csv")
pub_municipios_dic <- read_csv("pub_municipios_dic.csv")

dbGetQuery(con, "DROP TABLE clean.pub_diccionario_programas;")
dbWriteTable(con, c("clean",'pub_diccionario_programas'),pub_municipios_dic, row.names=FALSE)

##########################
#### Amenazas 
##########################

# LOAD FROM RAW
amenazas_naturales = as_tibble(dbGetQuery(con, "select * from clean.cenapred_app_municipios;"))