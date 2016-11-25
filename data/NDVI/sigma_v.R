##### first try for doing Sigma_v statistis
library(dplyr)
library(ggplot2)
library(xts)
#library(scales)
setwd('/home/javier/Documents/Dropbox/politica_preventiva/')
#source('data/NDVI/ndvi_utils.R')
rm(list=ls())
colClasses <- c(rep("character", 6), rep("numeric", 5), rep("character", 2))
avance <- read.csv('data/carencia-alimenticia/avance_agricola_maiz_clean.csv', colClasses = colClasses)
colClasses <- c(rep("character",2), "numeric", rep("character", 3))
ndvi <- read.csv('data/NDVI/municip_agrzones_ndvi.csv', colClasses = colClasses)
ndvi$DATE <- as.Date(paste(ndvi$DAY, ndvi$YEAR, sep="-"), format="%j-%Y")
#ndvi_g <- read.csv('data/NDVI/municip_wide_ndvi.csv', colClasses = colClasses)
#ndvi_g$DATE <- as.Date(paste(ndvi_g$DAY, ndvi_g$YEAR, sep="-"), format="%j-%Y")
#ndvi <- merge(ndvi, select(ndvi_g, CVE_MUN, DATE, value), by=c('CVE_MUN', 'DATE'), suffixes=c('', '_mun') )

colnames(avance)[1:2] <- c('EDO', 'CVE_MUN')
avance$CVE_MUN <- paste0(avance$EDO, avance$CVE_MUN)
avance$mes <- as.character(avance$mes)
avance$anio <- as.character(avance$anio)
avance$DATE <- paste(rep('28', nrow(avance)), avance$mes, avance$anio, sep='-')
avance$DATE <- as.Date(avance$DATE, format='%d-%m-%Y')
avance$DATE_G <- paste(rep('01', nrow(avance)), avance$mes_g, avance$anio_g, sep='-')
avance$DATE_G <- as.Date(avance$DATE_G, format='%d-%m-%Y')
avance$rendim <- avance$sup_cosech/avance$sup_sembrada

#ndvi3 <- melt(ndvi)
#ggplot(subset(ndvi, CVE_MUN %in% c('09014', '09013'))) + geom_line(aes(DATE, value), col='red', label='NDVI municipial') +
#  geom_line(aes(DATE, value_mun), label='NDVI total agricola') + facet_wrap(~NOM_MUN)

ggplot(subset(ndvi, EDO == '01')) +  geom_line(aes(x = DATE, y = NDVI_MUN, col=NOM_MUN)) + scale_x_date() +
  ggtitle('NDVI Histórico de Aguascalientes') 

avanceT <- filter(avance, moda_hidr == 'T') 
avanceT$indic <- 0
avanceT$indic[avanceT$mes_g == 1] <- 1

ndvi2 <- ndvi
ndvi_s <- cbind(select(ndvi2, CVE_MUN, EDO, value, DATE), ciclo = rep('PV', nrow(ndvi2)), CODIGO = rep('NDVI', nrow(ndvi2)))
avanceT$value <- avanceT$rendim
avanceT_s <- cbind(select(avanceT, CVE_MUN, EDO, value, DATE, ciclo), CODIGO = rep('Perdida', nrow(avanceT)))
intento <- rbind(ndvi_s, avanceT_s)
intento$mes <- months(intento$DATE, abbreviate = T)
ggplot(subset(intento, CVE_MUN=='09009')) + geom_line(aes(DATE, value, col=CODIGO)) +
  geom_text(aes(DATE, value, label=mes), size=2.7, check_overlap = T) + ggtitle('Porcentaje de superficie de temporal perdida\ny NDVI en Calvillo, Ags.')


min_date <- function(x, DATE, greater=T) {
  # from a vectoer of dates, get the most similar to a certain date. 
  # x: vector of dates
  # DATE: date to compare
  if (greater == T){
    x <- x[x >= DATE]
    return(min(x))
  } else {
    x <- x[x <= DATE]
    return(min(x))
  }
}

df1 <- avanceT %>% filter(CVE_MUN == '01003') %>% arrange(DATE)
ndvidf <- ndvi %>% filter(CVE_MUN == '01003') %>% arrange(DATE)
ndvits <- xts(ndvidf$value, ndvidf$DATE)
sigma_v <- function(ndvi, onset, lag, LAP, monthly = T){
  # Calculates sigma_v for a whole period 
  # onset must be a DATE (any date works)
  # ndvi must be an xts
  # lag and LAP are postive integers (each one is equivalent to 16 days)
  # if monthly, return a vector with a montly count of sigma_v values
  if (lag < 0) {lag <- 0}
  lag <- 16*lag
  if (LAP < 1) {LAP <- 1}
  LAP <- 16*LAP
  dates <- index(ndvi)
  onset_date <- min_date(dates, onset)
  ndvi_onset <- as.numeric(ndvi[onset_date])
  ndvi <- ndvi - ndvi_onset
  ndvi_start <- min_date(dates, onset + lag)
  ndvi_end <- min_date(dates, onset + lag + LAP)
  date_range <- paste(ndvi_start, ndvi_end, sep='/')
  ndvi_s <- ndvi[date_range] 
  if (monthly == T){
    ndvi_s <- apply.monthly(ndvi_s, sum)
    ndvi_s <- cumsum(ndvi_s)
    return(ndvi_s)
  } else{
    return(cumsum(ndvi_s))
  }
}
sigma_year <- function(ndvits, start_n, end_n){
  # Fill missing values in time series with zeroes
  dates <- seq.Date(start_n, end_n, by='month')
  dates <- xts(rep(0,length(dates)), dates) 
  dates <- merge(dates, ndvits,fill=0, all=T)
  dates <- dates[,'ndvits'] %>% apply.monthly(sum)
  return(dates)
}

fill_avance_dates <- function(df, start_month, start_year, end_month, end_year){
  # 
  dates <- fill_dates(start_month, start_year, end_month, end_year)
  df <- xts(select(df, -DATE), db$DATE)
  df <- merge(df, dates, fill=0)
  return(df) 
}
a <- sigma_v(ndvits, onset, lag=3, LAP=4)
fill_dates <- function(start_month, start_year, end_month, end_year, day='28'){
  # Create a sequence of dates from start month/year to end month/year (day = 28 is default because of avance agricola)
  # Returns: 
  start_d = as.Date(paste(start_year, start_month, day, sep='/'))
  end_d = as.Date(paste(end_year, end_month, day, sep='/'))
  dates <- xts(,seq.Date(start_d, end_d, by='month'))
  return(dates)
}

db <- avance %>% filter(CVE_MUN == '25003', moda_hidr == 'T', ciclo == 'PV') %>% 
  select(DATE, sup_sembrada, sup_cosech, sup_siniest, produccion, mes_g, anio_g) %>% arrange(DATE)
start_month <- format(db[1,1], '%m')
start_year <- format(db[1,1], '%Y')
end_month <- format(db[nrow(db),1], '%m')
end_year <- format(db[nrow(db),1], '%Y')
db <- fill_avance_dates(db, start_month, start_year, end_month, end_year)
dbf <- data.frame(db, DATE =index(db))
mdb <- melt(dbf, id='DATE')
ggplot(mdb) + geom_line(aes(DATE, value, col=variable, linetype=variable)) +
  geom_text(aes(DATE, value, label=format(DATE, '%m')), size=2.7, check_overlap = T)

cumulative_missing_cicles <- function(dfts, name_mes = 'mes_g', name_anio= 'anio_g') {
  # given an xts with missing agricultural months and years, add missing info 
  # NOTE: the first observation must be complete
  
  # obtain limits to fil dates
  start.month <- dfts[1, name_mes]
  start.year <- dfts[1, name_anio]
  end.month <- dfts[nrow(dfts), name_mes]
  end.year <- dfts[nrow(dfts), name_anio]
  
  # get vector of dates
  dates <- fill_dates(start.month, start.year, end.month, end.year) %>% index
  
  # obtain months and years
  months <- format(dates, '%m')
  years <- format(dates, '%Y')
  return(xts(cbind(months, years), index(dfts)))
}

db <- merge(db, cumulative_missing_cicles(db))

colMax <- function(data) sapply(data, max, na.rm = TRUE)

cumulative_missing_production <- function(dbts){
  #dbts must be a dataframe. This function needs to be passed a uniquely determined time series (no cicle, muns or hidro overlap)
  n <- nrow(dbts)
  for (i in 2:n){
    data <- rbind(dbts[i,], dbts[i-1,])
    dbts[i,] <- colMax(data)
  }
  return(dbts)
}





end(db$DATE)



