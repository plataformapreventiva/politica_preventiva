library(raster)
library(rgdal)
library(ggplot2)
library(scales)

zfill <- function(x, n=5){
  x <- as.character(x)
  z <- n - nchar(x)
  if (z > 0) {
    z <- paste0(rep(0,times=z), collapse = '')
    x <- paste0(z, x, collapse = '')
  }
  return(x)
}

myZonal <- function (x, z, stat, digits = 0, na.rm = TRUE, 
                     ...) { 
  library(data.table)
  fun <- match.fun(stat) 
  vals <- getValues(x) 
  zones <- round(getValues(z), digits = digits) 
  rDT <- data.table(vals, z=zones) 
  setkey(rDT, z) 
  rDT[, lapply(.SD, fun, na.rm=T), by=z] 
} 
border <- shapefile("/home/javier/MODIS_DATA/Geotiffs/agricultura_shape/agricultura_shape.shp") 
ZonalPipe<- function (path.in.shp, path.in.r, path.out.r, path.out.shp, zone.attribute, stat, landcrop = F){
  # 1/ Rasterize using GDAL
  # Gdal_rasterize
  r <- stack(path.in.r)
  ext<-extent(r)
  ext<-paste(ext[1], ext[3], ext[2], ext[4])
  res<-paste(res(r)[1], res(r)[2])
  
  #### cut shapefile for land use
  if (landcrop == T){
    #border <- shapefile("/home/javier/MODIS_DATA/Geotiffs/agricultura_shape/agricultura_shape.shp") 
    r <- crop(r,border) 
    r <- mask(r,border)
  }
  # command<-'gdal_rasterize'
  # command<-paste(command, "--config GDAL_CACHEMAX 2000") #Speed-up with more cache (avice: max 1/3 of your total RAM)
  # command<-paste(command, "-a", zone.attribute) #Identifies an attribute field on the features to be used for a burn in value. The value will be burned into all output bands.
  # command<-paste(command, "-te", as.character(ext)) #(GDAL >= 1.8.0) set georeferenced extents. The values must be expressed in georeferenced units. If not specified, the extent of the output file will be the extent of the vector layers.
  # command<-paste(command, "-tr", res) #(GDAL >= 1.8.0) set target resolution. The values must be expressed in georeferenced units. Both must be positive values.
  # command<-paste(command, path.in.shp)
  # command<-paste(command, path.out.r)
  # 
  # system(command)
  
  # Zonal Stat using myZonal function
  zone<-raster(path.out.r)
  name <- substr(path.in.r, 42, 49)
  Zstat<-data.frame(myZonal(r, zone, stat))
  Zstat$z <- sapply(Zstat$z, zfill)
  colnames(Zstat)[2:length(Zstat)]<-paste0("NDVI", "_",name)
  return(Zstat)
}

get_file_names <- function(path, pattern){
  dlist <- dir(path,pattern="DOY") 
  flsp_total <- c()
  for (i in 1:length(dlist)){
    fold <- paste(path, dlist[i], sep="")  # the respective DOY-folder
    fls <- dir(fold, pattern=pattern)       # all files that are available in the respective DOY-folder
    flsp <-paste(fold, fls, sep="/")      # all files that are available in the respective DOY-folder with complete path name
    flsp_total <- c(flsp_total, flsp)
  }
  to_order <- c()
  for (i in 1:length(flsp_total)){
    to_order <- c(to_order, paste(substr(flsp_total[i],46,49), substr(flsp_total[i],42,44), sep="_")) ### NOTA: HACER CON REGEX PARA SELECCIONAR
  }
  flsp <- flsp_total[order(to_order)]
}

path <- "/home/javier/MODIS_DATA/Geotiffs/"
pattern = "[0-9]_NDVI_smooth.tif$"
flsp <- get_file_names(path, pattern)
path.in.shp<-"/home/javier/MODIS_DATA/Geotiffs/shape_corr/shape_muns.shp"
path.out.r<-"/home/javier/MODIS_DATA/Geotiffs/first_smooth_stat.tif"
path.out.shp<-"/home/javier/MODIS_DATA/Geotiffs/shape_ftry/ftry16.shp"
zone.attribute<-"CVE_MUN"

shp <- readOGR(path.in.shp, layer= sub("^([^.]*).*", "\\1", basename(path.in.shp)))
data <- shp@data
rm(list=c('shp'))


for (i in 1:length(flsp)){
  path.in.r <- flsp[i]
  start <- Sys.time()
  Zstat <- ZonalPipe(path.in.shp, path.in.r, path.out.r, path.out.shp, zone.attribute, stat="mean", landcrop = T)
  data <- merge(data, Zstat, by.x = 'CVE_MUN', by.y='z', all.x=T, all.y=F)
  end <- Sys.time()- start
  print(paste(i, 'tiempo: ', end))
}
Zstat <- melt(data, id.vars = c('CVE_MUN', 'NOM_MUN'))

Zstat$DAY <- substr(Zstat$variable, 6,8)
Zstat$YEAR <- substr(Zstat$variable, 10, 13)
Zstat$DATE <- paste0(Zstat$DAY, '-', Zstat$YEAR)
Zstat$DATE <- as.Date(Zstat$DATE, format='%j-%Y')
#Zstat$DATE <- lapply(Zstat$DATE, as.Date, format='%j-%Y')
Zstat$variable <- NULL
Zstat$EDO <- substr(Zstat$CVE_MUN, 1,2)

ggplot(subset(Zstat, EDO == '09'), aes(x = DATE, y = value, col=CVE_MUN)) + 
  geom_line() +
  scale_x_date(labels = date_format("%j-%Y")) + facet_wrap(~CVE_MUN)
#write.csv(Zstat, '/home/javier/Documents/Dropbox/politica_preventiva/data/NDVI/municip_agrzones_ndvi.csv', row.names=F)


##### EJEMPLOS EXPLORATORIOS
path <- "/home/javier/MODIS_DATA/Geotiffs/"
pattern = "[0-9]_NDVI_smooth.tif$"
flsp <- get_file_names(path, pattern)

path.in.shp<-"/home/javier/MODIS_DATA/Geotiffs/shape_corr/shape_muns.shp"
path.in.r<-"/home/javier/MODIS_DATA/Geotiffs/DOY_017/017_2004_NDVI_smooth.tif" #or path.in.r<-list.files("/home/, pattern=".tif$")
path.out.r<-"/home/javier/MODIS_DATA/Geotiffs/first_smooth_stat.tif"
path.out.shp<-"/home/javier/MODIS_DATA/Geotiffs/shape_ftry/ftry16.shp"
zone.attribute<-"CVE_MUN"

Zstat <- ZonalPipe(path.in.shp, path.in.r, path.out.r, path.out.shp, zone.attribute, stat="mean", landcrop = T)
Zstat2 <- ZonalPipe(path.in.shp, path.in.r, path.out.r, path.out.shp, zone.attribute, stat="mean")
path.in.r<-"/home/javier/MODIS_DATA/Geotiffs/DOY_017/017_2016_NDVI_smooth.tif"
Z_stat16 <- ZonalPipe(path.in.shp, path.in.r, path.out.r, path.out.shp, zone.attribute, stat="mean")
Z_stat16agr <- ZonalPipe(path.in.shp, path.in.r, path.out.r, path.out.shp, zone.attribute, stat="mean", landcrop = T)

Zstat_tot <- cbind(Zstat, Zstat2$NDVI_mean, Z_stat16$NDVI_mean, Z_stat16agr$NDVI_mean)
colnames(Zstat_tot) <- c('MUN', 'AGR_2004', 'TOT_2004', 'TOT_2016', 'AGR_2016')
Zstat_tot <- melt(Zstat_tot)
ggplot(Zstat_tot) + geom_density(aes(value, colour=variable))
Zstat_tot$EDO <- substr(Zstat_tot$MUN, 0,2)
ggplot(subset(Zstat_tot, variable %in% c('TOT_2004', 'TOT_2016'))) + geom_density(aes(value, colour=variable)) + facet_wrap(~EDO)


shp <- readOGR(path.in.shp, layer= sub("^([^.]*).*", "\\1", basename(path.in.shp)))
data <- shp@data
Zstat_tot <- cbind(Zstat, Zstat2$NDVI_mean, Z_stat16$NDVI_mean, Z_stat16agr$NDVI_mean)
Zstat_tot <- melt(Zstat_tot)
ggplot(Zstat_tot) + geom_density(aes(value, colour=variable))
colnames(Zstat_tot) <- c('MUN', 'AGR_2004', 'TOT_2004', 'TOT_2016', 'AGR_2016')
data <- merge(data, Zstat, by.x = 'CVE_MUN', by.y='z', all.x=T, all.y=F)
shp@data <- data.frame(shp@data, Zstat[match(shp@data[,zone.attribute], Zstat[, "z"]),])
writeOGR(shp, path.out.shp, layer= sub("^([^.]*).*", "\\1", basename(path.in.shp)), driver="ESRI Shapefile", overwrite_layer = T, check_exists = T)

ZonalPipe(path.in.shp, path.in.r, path.out.r, path.out.shp, zone.attribute, stat="mean")
  