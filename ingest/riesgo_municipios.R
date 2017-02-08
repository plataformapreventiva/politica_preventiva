###############################################################################
# Riesgo Alimentario Municipios
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
## Riesgo Municipios
##################

municipios = as_tibble(dbGetQuery(con, "select * from clean.riesgo_alimentario_municipios;"))
colnames(municipios) = dbSafeNames(colnames(municipios))