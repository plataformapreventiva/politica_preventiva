#This script creates zonas statistics for ONE file based on the attributes of a shapefile. 
library(raster)
library(rgdal)
zfill <- function(x){
  x <- as.character(x)
  z <- 5 - nchar(x)
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

ZonalPipe<- function (path.in.shp, path.in.r, path.out.r, path.out.shp, zone.attribute, stat){
  
  # 1/ Rasterize using GDAL
  
  #Initiate parameter
  r<-stack(path.in.r)
  
  ext<-extent(r)
  ext<-paste(ext[1], ext[3], ext[2], ext[4])
  
  res<-paste(res(r)[1], res(r)[2])
  
  #Gdal_rasterize
  command<-'gdal_rasterize'
  command<-paste(command, "--config GDAL_CACHEMAX 2000") #Speed-up with more cache (avice: max 1/3 of your total RAM)
  command<-paste(command, "-a", zone.attribute) #Identifies an attribute field on the features to be used for a burn in value. The value will be burned into all output bands.
  command<-paste(command, "-te", as.character(ext)) #(GDAL >= 1.8.0) set georeferenced extents. The values must be expressed in georeferenced units. If not specified, the extent of the output file will be the extent of the vector layers.
  command<-paste(command, "-tr", res) #(GDAL >= 1.8.0) set target resolution. The values must be expressed in georeferenced units. Both must be positive values.
  command<-paste(command, path.in.shp)
  command<-paste(command, path.out.r)
  
  system(command)
  
  # 2/ Zonal Stat using myZonal function
  zone<-raster(path.out.r)
  
  Zstat<-data.frame(myZonal(r, zone, stat))
  Zstat$z <- sapply(Zstat$z, zfill)
  colnames(Zstat)[2:length(Zstat)]<-paste0("NDVI", "_",stat)
  
  # 3/ Merge data in the shapefile and write it
  shp<-readOGR(path.in.shp, layer= sub("^([^.]*).*", "\\1", basename(path.in.shp)))
  data <- merge(shp@data, Zstat, by.x = 'CVE_MUN', by.y='z', all.x=T, all.y=F)
  shp@data <- data.frame(shp@data, Zstat[match(shp@data[,zone.attribute], Zstat[, "z"]),])
  
  writeOGR(shp, path.out.shp, layer= sub("^([^.]*).*", "\\1", basename(path.in.shp)), driver="ESRI Shapefile", overwrite_layer = T, check_exists = T)
}

path.in.shp<-"/home/javier/MODIS_DATA/Geotiffs/shape_corr/shape_muns.shp"
path.in.r<-"/home/javier/MODIS_DATA/Geotiffs/DOY_193/193_2016_NDVI_smooth.tif" #or path.in.r<-list.files("/home/, pattern=".tif$")
path.out.r<-"/home/javier/MODIS_DATA/Geotiffs/first_smooth_stat.tif"
path.out.shp<-"/home/javier/MODIS_DATA/Geotiffs/shape_ftry/ftry16.shp"
zone.attribute<-"CVE_MUN"

ZonalPipe(path.in.shp, path.in.r, path.out.r, path.out.shp, zone.attribute, stat="mean")

#With
#path.in.shp: Shapefile with zone (INPUT)
#path.in.r: Raster from which the stats have to be computed (INPUT)
#path.out.r: Path of path.in.shp converted in raster (intermediate OUTPUT)
#path.out.shp: Path of path.in.shp with stat value (OUTPUT)
#zone.attribute: Attribute name of path.in.shp corresponding to the zones (ID, Country...)
#stat: function to summary path.in.r values ("mean", "sum"...)