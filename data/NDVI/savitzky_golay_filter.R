library(raster)
library(rgdal)
library(sp)
library(signal) #includes s
library(zoo)
library(timeSeries) 

start <- c(2004, 1)
end <- c(2016,20)

pixel_smooth <- function(x) {
  v <- as.vector(x)
  # remove atypical values (smaller NDVI than .01) and replace 
  #l_z <- length(v[v < .01]) 
  #if (length(v[!is.na(v)]) > 0){
  #  l_z <- length(v[v < .01]) 
  #  v[v < .01] <- rep(mean(v, na.rm=T), l_z)
  #}
  #v[v < .01] <- rep(mean(v, na.rm=T), l_z)
  # replace NA's with mean value 
  v <- substituteNA(v, type="mean")
  # create time series
  MODIS.ts2 <-  ts(v, start=start, end=end, frequency=23)
  # apply filter 
  x <- sgolayfilt(MODIS.ts2, p=3, n=5)
}

path <- "/home/javier/MODIS_DATA/Geotiffs/"
dlist <- dir(path,pattern="DOY") 
flsp_total <- c()
for (i in 1:length(dlist)){
  fold <- paste(path, dlist[i], sep="")  # the respective DOY-folder
  fls <- dir(fold, pattern="[0-9]_NDVI.tif")       # all files that are available in the respective DOY-folder
  flsp <-paste(fold, fls, sep="/")      # all files that are available in the respective DOY-folder with complete path name
  flsp_total <- c(flsp_total, flsp)
}
# NOTE: Files are stored via julian calendars 
# Files are located in DOY_DDD/DDD_YYYY_NDVI.tif format
to_order <- c()
for (i in 1:length(flsp_total)){
  to_order <- c(to_order, paste(substr(flsp_total[i],46,49), substr(flsp_total[i],42,44), sep="_")) ### NOTA: HACER CON REGEX PARA SELECCIONAR
}

flsp <- flsp_total[order(to_order)]

MODIS <- stack(flsp) 
#MODIS2 <- crop(MODIS, extent(MODIS, 1000, 1030, 1500, 1530))

MODIS.filtered <- calc(MODIS, pixel_smooth)

# Save layers
for (k in 1:nlayers(MODIS.filtered)) {     # start of the inner for-loop
  r <- nchar(flsp[k])
  path_s <- substr(flsp[k], 1, r-4)
  writeRaster(MODIS.filtered[[k]], filename=paste(path_s, '_smooth.tiff',sep=""), format="GTiff", overwrite=TRUE)        # in case you would like to have Envi files (Attention: note the datatype)
}


# GRAPH
filtered <- extract(MODIS.filtered, c(105))[1:295]
unfiltered <- extract(MODIS2, c(105))[1:295]
toplot <- data.frame(V1 = seq(1,295), raw = unfiltered, smooth = filtered)
ggplot(toplot) + geom_line(aes(V1, unfiltered), col='red')  + geom_line(aes(V1, filtered), col='blue') +
  geom_point(aes(V1, unfiltered), col='red')+ geom_point(aes(V1, filtered), col='blue') +
  geom_text(aes(V1, filtered, label=as.character(V1%%23)))


plot(sg.ts)