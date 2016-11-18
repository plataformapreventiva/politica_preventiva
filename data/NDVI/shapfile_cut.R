### For cutting raster images with excess info (from central america and the us) into the shape of Mexico
### Note: since MODIS REPROJECTION TOOLS does not have the official datum of INEGI, the shapefile must be modified slightly 
### to have WGS83 datum
### Yields an image of normalized NDVI and an image of VCI
setwd('/home/javier/MODIS_DATA/Geotiffs/')
library(raster)
library(rgdal)
rm(list=ls())
# load borders 
border<-shapefile("shape/shape_muns.sh.shp") 
# download country borders as shapefiles http://www.gadm.org/download

path <- "/home/javier/MODIS_DATA/Geotiffs/"
dlist <- dir(path,pattern="DOY") 

pb <- txtProgressBar(min=0, max=length(dlist), style=1) # this creates a progress bar in the Console, 
#which ends at the end of the loop. The proegress bar looks like this: =========
setTxtProgressBar (pb, 0)
for (i in 16:length(dlist)) {            # start of the outer for-loop
  fold <- paste(path, dlist[i], sep="")  # the respective DOY-folder
  fls <- dir(fold, pattern="[0-9].tif")       # all files that are available in the respective DOY-folder
  flsp <-paste(fold, fls, sep="/")      # all files that are available in the respective DOY-folder with complete path name
  ndvistack <- stack(flsp) #creates a layer stack of all files within the DOY folder
  ndviresize <- crop(ndvistack,border) #resizes the layer stack to the rectangular extent of the border shapefile
  ndvimask <- mask(ndviresize,border) # masks the layer stack using the border shapefile
  ndvi <- ndvimask*0.0001 #rescaling of MODIS data
  ndvi[ndvi == -0.3] <- NA #Fill value(-0,3) in NA
  ndvi[ndvi < (-0.2)] <- NA # as valid range is -0.2 -1 , all values smaller than -0,2 are masked out
  
  # extracting max and min value for each pixel
  ndvimax <- stackApply (ndvi, rep (1, nlayers (ndvi)),max, na.rm=F) #calculating the maximum value for the layer stack for each indivisual pixel
  ndvimin <- stackApply (ndvi, rep (1, nlayers (ndvi)), min, na.rm=F) #calculating the minimum value for the layer stack for each indivisual pixel
  
  # If na.rm is FALSE an NA value in any of the arguments will cause a value of NA to be returned, otherwise NA values are ignored.
  # https://stat.ethz.ch/R-manual/R-devel/library/base/html/Extremes.html
  
  z <- ndvimax - ndvimin # aggregation of the determinator
  
  VCI_all <- ((ndvi-ndvimin)/z)*100 #calculating VCI
  
  my_palette <- colorRampPalette(c("red", "yellow", "lightgreen")) #definition of the color scheme of the resulting maps
  
  
  
  for (k in 1:nlayers(ndvi)) {     # start of the inner for-loop
    
    year <- substr(fls[k],5,8) #extracting the fifth to eigths letter of the filename, which is the year (cf. data preparation above)
    doy <- substr(fls[k],1,3) #extracting the first to third letter of the filename, which is the DOY (cf. data preparation above)
    
    
    writeRaster(VCI_all[[k]], filename=paste(fold,"/",doy,"_",year, '_VCI.tiff',sep=""), format="GTiff", overwrite=TRUE)        # in case you would like to have Envi files (Attention: note the datatype)
    jpeg(filename=paste(fold,"/",doy,"_",year,".jpg",sep=""), quality = 100) #writes the jpg maps and names the files autmatically accoring to the pattern DOY_YYYY
    
    plot(VCI_all[[k]],zlim=c(0,100), col=my_palette(101),main=paste(doy," VCI "," (NDVI) ",year,sep=""))#automizes the title of the plot. ToDo: Adjust the file naming according to the data you are processing! E.g. if you base your VCI on EVI data, write (EVI) instead of (NDVI)
    
    dev.off()
    
    
    writeRaster(ndvi[[k]], filename=paste(fold,"/",doy,"_",year,"_NDVI.tif",sep=""), format="GTiff", overwrite=TRUE) #writes the geotiff and automizes the file naming according to the pattern DOY_YYYY
  }       # end of the inner for-loop
  
  
  setTxtProgressBar (pb, i)
}                         # end of the outer for-loop
