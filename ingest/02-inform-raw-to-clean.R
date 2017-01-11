###############################################################################
# Clean to HAZARD - INDEX SEMANTIC 
# Get data from clean databases to create the semantic index db
###############################################################################

rm(list=ls())
suppressPackageStartupMessages({
  library(ggplot2)
  library(tidyverse)
  library(lubridate)
  library(stringr)
  library('RPostgreSQL')
  source("./utils_shameful.R")
})


##################
## Create Connection to DB
##################
conf <- fromJSON("../conf/conf_profile.json")
pg = dbDriver("PostgreSQL")
con = dbConnect(pg, user=conf$PGUSER, password=conf$PGPASSWORD,
                host=conf$PGHOST, port=5432, dbname=conf$PGDATABASE)



##########################
#### ÍNDICE DE CAPACIDADES
##########################

# FALTA - JAVI

##########################
#### ÍNDICE DE AMENAZAS
##########################

# FALTA - JAVI



##########################
#### ÍNDICE DE VULNERABILIDAD
##########################

##### Socioeconómica
rezago_soc = as_tibble(dbGetQuery(con, 'select cve_muni, irez_soc15 from "raw"."ISR_municipios";')) 
gini = as_tibble(dbGetQuery(con, "select cve_muni, gini_10 from raw.coneval_municipios;"))


##### Vulnerable groups
## Unprooted
indigenas <- read_csv("http://www.cdi.gob.mx/datosabiertos/2010/pob-indi-mpio-2010.csv",skip = 1, col_names = FALSE) %>%
  mutate(cve_muni = paste(X1,X3,sep = "")) %>%  select(cve_muni, X7) %>%
  rename(pob_indigena=X7)
adultos_mayores <- as_tibble(dbGetQuery(con, "select cve_muni, pob as adultos FROM raw.conapo_proyecciones_poblacion where edad = '65+' and anio = 2016;"))
unprooted_people <- full_join(indigenas,adultos_mayores) %>% rowwise() %>% mutate(unprooted = sum(pob_indigena, adultos,na.rm = TRUE)) %>%
  select(cve_muni,unprooted)


## Other
ic_asalud_alim = as_tibble(dbGetQuery(con, "select cve_muni, ic_asalud,  ic_ali from raw.coneval_municipios;"))
infantil <- read_csv("../data/grupos_vulnerables/mortalidad_infantil_2005.csv",skip = 4,col_names = FALSE) %>%
  filter(X1>0) %>%
  select(X1,X3) %>% rename(cve_muni=X1, tasa_mortalidad_infantil=X3) %>%
  mutate(cve_muni = str_pad(cve_muni,5,pad="0"),tasa_mortalidad_infantil_esc = normalize(tasa_mortalidad_infantil)) %>%
  select(cve_muni,tasa_mortalidad_infantil_esc)
precio_maiz <- read_csv("../data/precios_maiz/precio_maiz_2015.csv") 
full_db <- full_join(rezago_soc, gini) %>% full_join(unprooted_people) %>%full_join(ic_asalud_alim) %>% full_join(infantil) %>%
  full_join(precio_maiz)
full_db <- full_db %>% mutate(irez_soc15 = na.locf(irez_soc15),gini_10 = na.locf(gini_10), 
                              unprooted = na.locf(unprooted), ic_asalud = na.locf(ic_asalud), precio_maiz_2015 = na.locf(precio_maiz_2015))
full_db <- full_db %>% mutate(irez_soc15 = normalize(std(irez_soc15)),gini_10 = normalize(std(gini_10)),
                              unprooted = normalize(std(unprooted)), ic_asalud = normalize(std(ic_asalud)), precio_maiz_2015 = normalize(std(precio_maiz_2015)))
full_db <- full_db %>% rowwise() %>% mutate(socioeconomica = mean(irez_soc15,gini_10) , o_vulnerable = mean(ic_asalud,precio_maiz_2015)) 

#tasa_mortalidad_infantil_esc
full_db <- full_db %>%  rowwise() %>% mutate(vulnerable_groups = mean(unprooted, o_vulnerable),vulnerable = mean(socioeconomica,vulnerable_groups))
write.csv(full_db,"semantic_vulnerabilidad.csv",row.names = FALSE)
full_db <- read.csv("semantic_vulnerabilidad.csv")
dbWriteTable(con, c("raw",'semantic_vulnerabilidad'),full_db, row.names=FALSE)
full_db_dic <- read.csv("semantic_vulnerabilidad_dic.csv")
dbWriteTable(con, c("raw",'semantic_vulnerabilidad_dic'),full_db_dic, row.names=FALSE)


##########################
#### BASE SEMANTICA INDEX
##########################

semantic_index = as_tibble(dbGetQuery(con, 'select * from "raw"."semantic_index";')) 
write.csv(semantic_index,"./semantic_index.csv",row.names = FALSE)
