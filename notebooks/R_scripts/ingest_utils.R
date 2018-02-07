suppressPackageStartupMessages({
  library(psych)
  library(jsonlite)
  library(ggplot2)
  library(tidyverse)
  library(lubridate)
  require(zoo)
  library(stringr)
  library('RPostgreSQL')
  source("../../shameful/utils_shameful.R")
  library(foreign)
  library("rgdal")
  library("rgeos")
  library("dplyr")
})

##################
## Create Connection to DB
##################

conf <- fromJSON("../../config/conf_profile.json")
pg = dbDriver("PostgreSQL")
con = dbConnect(pg, user=conf$PGUSER, password=conf$PGPASSWORD,
                host=conf$PGHOST, port=5432, dbname=conf$PGDATABASE)


dbSafeNames = function(names) {
  names = gsub('_$','',names)
  names = gsub('[^a-z0-9]+','_',tolower(names))
  names[grepl("^[0-9]",names)] = paste0("p_",names[grepl("^[0-9]",names)])
  names = make.names(names, unique=TRUE, allow_=TRUE)
  names = gsub('.','_',names, fixed=TRUE)
  names
}

substrRight <- function(x, n){
  substr(x, nchar(x)-n+1, nchar(x))
}

dbSafeNames_explorador = function(names,n) {
  names = gsub('_$','',names)
  names = gsub('[^a-z0-9]+','_',tolower(names))
  names[grepl("^[0-9]",names)] = paste0("p_",names[grepl("^[0-9]",names)])
  names = make.names(names, unique=TRUE, allow_=TRUE)
  names = gsub('.','_',names, fixed=TRUE)
  names = substrRight(names, n)
  names
}


normalize <- function(x) {
  return ((x - min(x)) / (max(x) - min(x)))
}


standardize <- function(x){
  if(length(which(is.na(x)))==0) (x-mean(x))/sd(x) else
    (x-mean(x,na.rm=T))/sd(x,na.rm=T)
}

round_df <- function(df, digits) {
  nums <- vapply(df, is.numeric, FUN.VALUE = logical(1))
  
  df[,nums] <- round(df[,nums], digits = digits)
  
  (df)
}