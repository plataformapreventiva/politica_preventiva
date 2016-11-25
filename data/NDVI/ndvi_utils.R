########## COMMON USE FUNCTIONS
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
