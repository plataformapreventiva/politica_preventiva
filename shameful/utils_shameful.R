##################
## Basic Packages
##################
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



dbSafeNames = function(names) {
  names = gsub('[^a-z0-9]+','_',tolower(names))
  names = make.names(names, unique=TRUE, allow_=TRUE)
  names = gsub('.','_',names, fixed=TRUE)
  names
}

normalize <- function(x) {
  return ((x - min(x)) / (max(x) - min(x)))
}


std <- function(x){
  if(length(which(is.na(x)))==0) (x-mean(x))/sd(x) else
    (x-mean(x,na.rm=T))/sd(x,na.rm=T)
}
